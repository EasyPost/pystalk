from typing import List, Optional, Union
import time
import random
import socket
import logging

from .client import BeanstalkClient, BeanstalkError


RETRIABLE_ERRORS = ('INTERNAL_ERROR', 'OUT_OF_MEMORY')


def _get_time():
    return time.monotonic()


class NoMoreClients(Exception):
    def __str__(self):
        return "No clients can process requests at this time"


class ProductionPool(object):
    """A pool for producing jobs into a list of beanstalk servers. When an error occurs, job insertion
    will be re-attempted on the next server in the pool.

    :param clients: List of beanstalk client instances to use
    :param rotate_time: Number of seconds after which connections will be rotated (for load-balancing). Set to None
        to disable rotation
    :param backoff_time: Number of seconds after an error before a server will be reused
    :param shuffle: Randomly shuffle clients at initialization

    All clients should have a socket timeout set or else some errors will not be detected.

    NOTE: This will give you at-least-once deliverability (presuming at least one server is up), but can *easily*
    result in jobs being issued multiple times. Only use this functionality with idempotent jobs.

    This method of pooling is only suitable for use when *producing* jobs. For *consuming* jobs from a cluster of
    beanstalkd servers, consider the `pystalkworker` project.
    """
    def __init__(self, clients: List[BeanstalkClient], rotate_time: Optional[float] = 600.0,
                 backoff_time: float = 10.0, shuffle: bool = True):
        if not clients:
            raise ValueError('Must pass at least one BeanstalkClient')
        self._current_client_index = 0
        self._last_rotate_time = _get_time()
        self._clients = clients[:]
        if shuffle:
            random.shuffle(self._clients)
        self._last_failure = [None for _ in clients]
        self.current_tube: Optional[str] = None
        self.rotate_time = rotate_time
        self.backoff_time = backoff_time
        self.log = logging.getLogger('pystalk.ProductionPool')

    @classmethod
    def from_uris(cls, uris: List[str], socket_timeout: float = None, auto_decode: bool = False,
                  rotate_time: Optional[float] = 600.0, backoff_time: float = 10.0, shuffle: bool = True):
        """Construct a pool from a list of URIs. See `pystalk.client.Client.from_uri` for more information.

        :param uris: A list of URIs
        :param socket_timeout: Socket timeout to set on all constructed clients
        :param auto_decode: Whether bodies should be bytes (False) or strings (True)
        """
        return cls(
            clients=[BeanstalkClient.from_uri(uri, socket_timeout=socket_timeout, auto_decode=auto_decode)
                     for uri in uris],
            rotate_time=rotate_time,
            backoff_time=backoff_time,
            shuffle=shuffle
        )

    def use(self, tube: str):
        """Start producing jobs into the given tube.

        :param tube: Name of the tube to USE

        Subsequent calls to :func:`put_job` insert jobs into this tube.
        """
        self.current_tube = tube

    def _get_client(self):
        now = _get_time()
        start = 0
        if self.rotate_time:
            if now - self._last_rotate_time > self.rotate_time:
                start = 1
                self._last_rotate_time = now
        for index_offset in range(start, len(self._clients)):
            index = (self._current_client_index + index_offset) % len(self._clients)
            if self._last_failure[index] is None or now - self._last_failure[index] > self.backoff_time:
                self._current_client_index = index
                client = self._clients[index]
                if client.current_tube != self.current_tube:
                    client.use(self.current_tube)
                return index, client
        self.log.error('All clients are failed! %r', self._last_failure)
        raise NoMoreClients()

    def _mark_client_failed(self, client_index: int):
        self._last_failure[client_index] = _get_time()

    def _attempt_on_all_clients(self, thunk):
        while True:
            try:
                index, client = self._get_client()
                return thunk(client)
            except BeanstalkError as e:
                if e.message in RETRIABLE_ERRORS:
                    self.log.warning('error on server %r (%d): %r', client, index, e)
                    self._mark_client_failed(index)
                else:
                    raise
            except (socket.error) as e:
                self.log.warning('error on server %r (%d): %r', client, index, e)
                self._mark_client_failed(index)

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
