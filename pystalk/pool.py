from typing import List, Optional, Union
from collections import deque
import time
import random
import socket
import logging

import attr

from .client import BeanstalkClient, BeanstalkConnectionError, BeanstalkError


RETRIABLE_ERRORS = ('INTERNAL_ERROR', 'OUT_OF_MEMORY')


def _get_time():
    return time.monotonic()


class NoMoreClients(Exception):
    def __str__(self):
        return "No clients can process requests at this time"


@attr.s
class ClientRecord(object):
    client: BeanstalkClient = attr.ib()
    last_failed_at: Optional[float] = attr.ib(default=None)

    def is_ok(self, backoff_time, now=None):
        if now is None:
            now = _get_time()
        if self.last_failed_at is None:
            return True
        return now >= self.last_failed_at + backoff_time

    def mark_failed(self, now=None):
        if now is None:
            now = _get_time()
        self.last_failed_at = now


class ProductionPool(object):
    """A pool for producing jobs into a list of beanstalk servers. When an error occurs, job insertion
    will be re-attempted on the next server in the pool.

    :param clients: List of beanstalk client instances to use
    :param round_robin: If true, every insertion will go to a different server in the pool. If false,
        the server will only be changed when an exception occurs.
    :param backoff_time: Number of seconds after an error before a server will be reused
    :param initial_shuffle: Randomly shuffle clients at initialization

    All clients should have a socket timeout set or else some errors will not be detected.

    NOTE: This will give you at-least-once deliverability (presuming at least one server is up), but can *easily*
    result in jobs being issued multiple times. Only use this functionality with idempotent jobs.

    This method of pooling is only suitable for use when *producing* jobs. For *consuming* jobs from a cluster of
    beanstalkd servers, consider the `pystalkworker` project.
    """
    def __init__(self, clients: List[BeanstalkClient], round_robin: bool = True,
                 backoff_time: float = 10.0, initial_shuffle: bool = True):
        if not clients:
            raise ValueError('Must pass at least one BeanstalkClient')
        client_records = [ClientRecord(c) for c in clients]
        if initial_shuffle:
            random.shuffle(client_records)
        self._clients = deque(client_records)
        self.current_tube: Optional[str] = None
        self.round_robin = round_robin
        self.backoff_time = backoff_time
        self.log = logging.getLogger('pystalk.ProductionPool')

    @classmethod
    def from_uris(cls, uris: List[str], socket_timeout: Optional[float] = None, auto_decode: bool = False,
                  round_robin: bool = True, backoff_time: float = 10.0, initial_shuffle: bool = True):
        """Construct a pool from a list of URIs. See `pystalk.client.Client.from_uri` for more information.

        :param uris: A list of URIs
        :param socket_timeout: Socket timeout to set on all constructed clients
        :param auto_decode: Whether bodies should be bytes (False) or strings (True)
        """
        return cls(
            clients=[BeanstalkClient.from_uri(uri, socket_timeout=socket_timeout, auto_decode=auto_decode)
                     for uri in uris],
            round_robin=round_robin,
            backoff_time=backoff_time,
            initial_shuffle=initial_shuffle
        )

    def use(self, tube: str):
        """Start producing jobs into the given tube.

        :param tube: Name of the tube to USE

        Subsequent calls to :func:`put_job` insert jobs into this tube.
        """
        self.current_tube = tube

    def _get_client(self, attempted_client_ids=None):
        # Attempt to find the next live client.
        if attempted_client_ids is None:
            attempted_client_ids = set()
        now = _get_time()
        for _ in range(len(self._clients)):
            client_record = self._clients[0]
            if id(client_record) not in attempted_client_ids and client_record.is_ok(self.backoff_time, now=now):
                return client_record
            self._clients.rotate(-1)
        self.log.error('All clients are failed!')
        raise NoMoreClients()

    def _mark_client_failed(self, client_record, close_connection=False):
        if close_connection:
            try:
                client_record.client.close()
            except socket.error as e:
                self.log.warning('error closing failed client %r: %r', client_record, e)
        client_record.mark_failed()
        if self._clients[0] is client_record:
            self._clients.rotate(-1)

    def _attempt_on_all_clients(self, thunk):
        attempted_client_ids = set()
        while len(attempted_client_ids) < len(self._clients):
            client_record = self._get_client(attempted_client_ids)
            attempted_client_ids.add(id(client_record))
            try:
                if self.current_tube is not None and client_record.client.current_tube != self.current_tube:
                    client_record.client.use(self.current_tube)
                rv = thunk(client_record.client)
                if self.round_robin:
                    self._clients.rotate(-1)
                return rv
            except BeanstalkConnectionError as e:
                self.log.warning('error on server %r: %r', client_record, e)
                self._mark_client_failed(client_record, close_connection=True)
            except BeanstalkError as e:
                if e.message in RETRIABLE_ERRORS:
                    self.log.warning('error on server %r: %r', client_record, e)
                    self._mark_client_failed(client_record)
                else:
                    raise
            except socket.error as e:
                self.log.warning('error on server %r: %r', client_record, e)
                self._mark_client_failed(client_record, close_connection=True)
        self.log.error('All clients failed during request!')
        raise NoMoreClients()

    def put_job(self, data: Union[str, bytes], pri: int = 65536, delay: int = 0, ttr: int = 120):
        """Insert a new job into whatever queue is currently USEd

        :param data: Job body
        :type data: Text (either str which will be encoded as utf-8, or bytes which are already utf-8
        :param pri: Priority for the job
        :type pri: int
        :param delay: Delay in seconds before the job should be placed on the ready queue
        :type delay: int
        :param ttr: Time to reserve (how long a worker may work on this job before we assume the worker is blocked
            and give the job to another worker
        :type ttr: int

        .. seealso::

           :func:`put_job_into()`
              Put a job into a specific tube

           :func:`using()`
              Insert a job using an external guard
        """
        return self._attempt_on_all_clients(
            lambda client: client.put_job(data=data, pri=pri, delay=delay, ttr=120)
        )

    def put_job_into(self, tube_name: str, data: Union[str, bytes], pri: int = 65536,
                     delay: int = 0, ttr: int = 120):
        """Insert a new job into a specific queue. Wrapper around :func:`put_job`.

        :param tube_name: Tube name
        :type tube_name: str
        :param data: Job body
        :type data: Text (either str which will be encoded as utf-8, or bytes which are already utf-8
        :param pri: Priority for the job
        :type pri: int
        :param delay: Delay in seconds before the job should be placed on the ready queue
        :type delay: int
        :param ttr: Time to reserve (how long a worker may work on this job before we assume the worker is blocked
            and give the job to another worker
        :type ttr: int

        .. seealso::

           :func:`put_job()`
              Put a job into whatever the current tube is

           :func:`using()`
              Insert a job using an external guard
        """
        return self._attempt_on_all_clients(
            lambda client: client.put_job_into(tube_name=tube_name, data=data, pri=pri, delay=delay, ttr=120)
        )
