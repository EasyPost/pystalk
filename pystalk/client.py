from contextlib import contextmanager
import attr
import socket
import yaml


@attr.s(frozen=True)
class Job(object):
    """Structure holding a job returned from Beanstalk

    :ivar job_id: Opaque identifier for the job (to be passed to :func:`BeanstalkClient.release_job()` or
        :func:`BeanstalkClient.stats_job()`).

    :ivar job_data: Blob of the data. str if :attr:`BeanstalkClient.auto_decode` is True; otherwise bytes
    """

    job_id = attr.ib(validator=attr.validators.instance_of(int))

    job_data = attr.ib()


@attr.s(frozen=True, hash=True, cmp=True)
class BeanstalkError(Exception):
    """Common error raised when something goes wrong with beanstalk"""

    message = attr.ib(convert=lambda m: m.decode('ascii'))


def yaml_load(fo):
    # yaml.safe_load will never use the C loader; we have to detect it ourselves
    if hasattr(yaml, 'CSafeLoader'):
        return yaml.load(fo, Loader=yaml.CSafeLoader)
    else:
        return yaml.safe_load(fo)


@attr.s(frozen=True)
class BeanstalkInsertingProxy(object):
    """Proxy object yielded by :func:`BeanstalkClient.using()`"""

    beanstalk_client = attr.ib()
    tube = attr.ib()

    def put_job(self, data, pri=65536, delay=0, ttr=120):
        """Method to insert a job into the tube selected with :func:`BeanstalkClient.using`.

        :param data: Job body
        :type data: Text (either str which will be encoded as utf-8, or bytes which are already utf-8
        :param pri: Priority for the job
        :type pri: int
        :param delay: Delay in seconds before the job should be placed on the ready queue
        :type delay: int
        :param ttr: Time to reserve (how long a worker may work on this job before we assume the worker is blocked
            and give the job to another worker
        :type ttr: int
        """

        self.beanstalk_client.use(self.tube)
        return self.beanstalk_client.put_job(data=data, pri=pri, delay=delay, ttr=ttr)


class BeanstalkClient(object):
    """Simple wrapper around the Beanstalk API.

    :param host: Hostname or IP address to connect to
    :type host: str
    :param port: Port to connect to
    :type port: int
    :param socket_timeout: Timeout to set on the socket.
    :type socket_timeout: float
    :param auto_decode: Attempt to decode job bodies as UTF-8 when reading them
    :type auto_decode: bool

    Doesn't provide any fanciness for writing consumers or producers. Just lets you invoke methods to call beanstalk
    functions.

      .. warning::

         Setting socket timeout to a value lower than the value you pass to blocking functions like
         :func:`reserve_job()` will cause errors!
    """
    def __init__(self, host, port=11300, socket_timeout=None, auto_decode=False):
        """Construct a synchronous Beanstalk Client. Does not connect!"""
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self._reset_state()
        self.desired_tube = 'default'
        self.desired_watchlist = set(['default'])
        self.auto_decode = auto_decode

    def _reset_state(self):
        self._watchlist = set(['default'])
        self.current_tube = 'default'
        self.initial_watch = True
        self.socket = None

    def __repr__(self):
        return '{0}({1!r}, {2!r})'.format(self.__class__.__name__, self.host, self.port)  # pragma: no cover

    def __str__(self):
        return '{0} - watching:{1}, current:{2}'.format(  # pragma: no cover
            repr(self), self._watchlist, self.current_tube  # pragma: no cover
        )  # pragma: no cover

    @property
    def _socket(self):
        if self.socket is None:
            self.socket = socket.create_connection((self.host, self.port), timeout=self.socket_timeout)
            self._re_establish_use_watch()
        return self.socket

    def _re_establish_use_watch(self):
        """Call after a close/re-connect.

        Automatically re-establishes the USE and WATCH configs previously setup.
        """
        if self.current_tube != self.desired_tube:
            self.use(self.desired_tube)
        if self._watchlist != self.desired_watchlist:
            self.watchlist = self.desired_watchlist

    def close(self):
        """Close any open connection to the Beanstalk server.

        This object is still safe to use after calling :func:`close()` ; it will automatically reconnect
        and re-establish any open watches / uses.

        It is a logic error to close the connection while you have a reserved job
        """
        if self.socket is not None:
            self.socket.close()
            self._reset_state()

    @contextmanager
    def _sock_ctx(self):
        yield self._socket

    def _receive_data_with_prefix(self, prefix, sock):
        buf = b''
        target_len = len(prefix) + 28
        while b'\r\n' not in buf:
            message = sock.recv(target_len - len(buf))
            if not message:
                break
            buf += message
        if b' ' not in buf:
            error = buf.rstrip()
            raise BeanstalkError(error)
        first_word, rest = buf.split(b' ', 1)
        if first_word != prefix:
            raise BeanstalkError(first_word)
        return self._receive_data(sock, rest)

    def _receive_id_and_data_with_prefix(self, prefix, sock):
        buf = b''
        target_len = len(prefix) + 28
        while b'\r\n' not in buf:
            message = sock.recv(target_len - len(buf))
            if not message:
                break
            buf += message
        if b' ' not in buf:
            error = buf.rstrip()
            raise BeanstalkError(error)
        first_word, rest = buf.split(b' ', 1)
        if first_word != prefix:
            raise BeanstalkError(first_word)
        the_id, rest = rest.split(b' ', 1)
        return int(the_id), self._receive_data(sock, rest)

    def _receive_data(self, sock, initial=None):
        if initial is None:
            initial = sock.recv(12)
        byte_length, rest = initial.split(b'\r\n', 1)
        byte_length = int(byte_length) + 2
        buf = [rest]
        bytes_read = len(rest)
        while bytes_read < byte_length:
            message = sock.recv(min(4096, byte_length - bytes_read))
            if not message:
                break
            bytes_read += len(message)
            buf.append(message)
        bytez = b''.join(buf)[:-2]
        if self.auto_decode:
            return bytez.decode('utf-8')
        else:
            return bytez

    def _receive_id(self, sock):
        status, gid = self._receive_name(sock)
        return status, int(gid)

    def _receive_name(self, sock):
        message = sock.recv(1024)
        if b' ' in message:
            status, rest = message.split(b' ', 1)
            return status, rest.rstrip()
        else:
            raise BeanstalkError(message.rstrip())

    def _receive_word(self, sock, *expected_words):
        message = sock.recv(1024).rstrip()
        if message not in expected_words:
            raise BeanstalkError(message)
        return message

    def _send_message(self, message, sock):
        if isinstance(message, bytes):
            if not message.endswith(b'\r\n'):
                message += b'\r\n'
            return sock.sendall(message)
        else:
            if not message.endswith('\r\n'):
                message += '\r\n'
            return sock.sendall(message.encode('utf-8'))

    def list_tubes(self):
        """Return a list of tubes that this beanstalk instance knows about

        :rtype: list of tubes
        """
        with self._sock_ctx() as sock:
            self._send_message('list-tubes', sock)
            body = self._receive_data_with_prefix(b'OK', sock)
            tubes = yaml_load(body)
            return tubes

    def stats(self):
        """Return a dictionary with a bunch of instance-wide statistics

        :rtype: dict
        """
        with self._sock_ctx() as socket:
            self._send_message('stats', socket)
            body = self._receive_data_with_prefix(b'OK', socket)
            stats = yaml_load(body)
            return stats

    def put_job(self, data, pri=65536, delay=0, ttr=120):
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
        with self._sock_ctx() as socket:
            message = 'put {pri} {delay} {ttr} {datalen}\r\n'.format(
                pri=pri, delay=delay, ttr=ttr, datalen=len(data), data=data
            ).encode('utf-8')
            if not isinstance(data, bytes):
                data = data.encode('utf-8')
            message += data
            message += b'\r\n'
            self._send_message(message, socket)
            return self._receive_id(socket)

    def put_job_into(self, tube_name, data, pri=65536, delay=0, ttr=120):
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
        with self.using(tube_name) as inserter:
            return inserter.put_job(data=data, pri=pri, delay=delay, ttr=ttr)

    @property
    def watchlist(self):
        return self._watchlist

    @watchlist.setter
    def watchlist(self, tubes):
        """Set the watchlist to the given tubes

        :param tubes: A list of tubes to watch

        Automatically un-watches any tubes that are not on the target list
        """
        tubes = set(tubes)
        for tube in tubes - self._watchlist:
            self.watch(tube)
        for tube in self._watchlist - tubes:
            self.ignore(tube)

    def watch(self, tube):
        """Add the given tube to the watchlist.

        :param tube: Name of the tube to add to the watchlist

        Note: Initially, all connections are watching a tube named "default". If
        you manually call :func:`watch()`, we will un-watch the "default" tube.
        To keep it in your list, first call :func:`watch()` with the other tubes, then
        call :func:`watch()` with "default".
        """
        with self._sock_ctx() as socket:
            self.desired_watchlist.add(tube)
            if tube not in self._watchlist:
                self._send_message('watch {0}'.format(tube), socket)
                self._receive_id(socket)
                self._watchlist.add(tube)
                if self.initial_watch:
                    if tube != 'default':
                        self.ignore('default')
                    self.initial_watch = False

    def ignore(self, tube):
        """Remove the given tube from the watchlist.

        :param tube: Name of tube to remove from the watchlist

        If all tubes are :func:`ignore()` d, beanstalk will auto-add "default" to the watchlist
        to prevent the list from being empty. See :func:`watch()` for more unformation.
        """
        with self._sock_ctx() as socket:
            if tube not in self._watchlist:
                raise KeyError(tube)
            if tube != 'default':
                self.desired_watchlist.remove(tube)
            if tube in self._watchlist:
                self._send_message('ignore {0}'.format(tube), socket)
                self._receive_id(socket)
                self._watchlist.remove(tube)
            if not self._watchlist:
                self._watchlist.add('default')

    def stats_job(self, job_id):
        """Fetch statistics about a single job

        :rtype: dict
        """
        with self._sock_ctx() as socket:
            if hasattr(job_id, 'job_id'):
                job_id = job_id.job_id
            self._send_message('stats-job {0}'.format(job_id), socket)
            body = self._receive_data_with_prefix(b'OK', socket)
            job_status = yaml_load(body)
            return job_status

    def stats_tube(self, tube_name):
        """Fetch statistics about a single tube

        :param tube_name: Tube to fetch stats about
        :rtype: dict
        """
        with self._sock_ctx() as socket:
            self._send_message('stats-tube {0}'.format(tube_name), socket)
            body = self._receive_data_with_prefix(b'OK', socket)
            return yaml_load(body)

    def reserve_job(self, timeout=5):
        """Reserve a job for this connection. Blocks for TIMEOUT secionds and raises TIMED_OUT if no job was available

        :param timeout: Time to wait for a job, in seconds.
        :type timeout: int
        """
        timeout = int(timeout)
        if timeout >= self.socket_timeout:
            raise ValueError('reserve_job timeout must be < socket timeout')
        if not self._watchlist:
            raise ValueError('Select a tube or two before reserving a job')
        with self._sock_ctx() as socket:
            self._send_message('reserve-with-timeout {0}'.format(timeout), socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'RESERVED', socket)
            return Job(job_id, job_data)

    def peek_ready(self):
        """Peek at the job job on the ready queue.

        :rtype: :class:`Job`
        """
        with self._sock_ctx() as socket:
            self._send_message('peek-ready', socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'FOUND', socket)
            return Job(job_id, job_data)

    def peek_delayed(self):
        """Peek at the job job on the delayed queue"""
        with self._sock_ctx() as socket:
            self._send_message('peek-delayed', socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'FOUND', socket)
            return Job(job_id, job_data)

    def peek_buried(self):
        """Peek at the top job on the buried queue"""
        with self._sock_ctx() as socket:
            self._send_message('peek-buried', socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'FOUND', socket)
            return Job(job_id, job_data)

    def _common_iter(self, kallable, error):
        while True:
            try:
                job = kallable()
            except BeanstalkError as e:
                if e.message != error:
                    raise
                break
            yield job

    def reserve_iter(self):
        """Reserve jobs as an iterator. Ends iteration when there are no more jobs immediately available"""
        return self._common_iter(lambda: self.reserve_job(0), 'TIMED_OUT')

    def peek_delayed_iter(self):
        """Peek at delayed jobs in sequence"""
        return self._common_iter(self.peek_delayed, 'NOT_FOUND')

    def peek_buried_iter(self):
        """Peek at buried jobs in sequence"""
        return self._common_iter(self.peek_buried, 'NOT_FOUND')

    def delete_job(self, job_id):
        """Delete the given job id. The job must have been previously reserved by this connection"""
        if hasattr(job_id, 'job_id'):
            job_id = job_id.job_id
        with self._sock_ctx() as socket:
            self._send_message('delete {0}'.format(job_id), socket)
            self._receive_word(socket, b'DELETED')

    def bury_job(self, job_id, pri=65536):
        """Mark the given job_id as buried. The job must have been previously reserved by this connection

        :param job_id: Job to bury
        :param pri: Priority for the newly-buried job. If not passed, will keep its current priority
        :type pri: int
        """
        if hasattr(job_id, 'job_id'):
            job_id = job_id.job_id
        with self._sock_ctx() as socket:
            self._send_message('bury {0} {1}'.format(job_id, pri), socket)
            return self._receive_word(socket, b'BURIED')

    def release_job(self, job_id, pri=65536, delay=0):
        """Put a job back on the queue to be processed (indicating that you've aborted it)

        You can only release a job which you have reserved using :func:`reserve_job()` or :func:`reserve_iter()`.

        :param job_id: Job ID to return
        :param pri: New priority (if not passed, will use old priority)
        :type pri: int
        :param delay: New delay for job (if not passed, will use 0)
        :type delay: int
        """
        if hasattr(job_id, 'job_id'):
            job_id = job_id.job_id
        with self._sock_ctx() as socket:
            self._send_message('release {0} {1} {2}\r\n'.format(job_id, pri, delay), socket)
            return self._receive_word(socket, b'RELEASED', b'BURIED')

    def use(self, tube):
        """Start producing jobs into the given tube.

        :param tube: Name of the tube to USE

        Subsequent calls to :func:`put_job` insert jobs into this tube.
        """
        with self._sock_ctx() as socket:
            if self.current_tube != tube:
                self.desired_tube = tube
                self._send_message('use {0}'.format(tube), socket)
                self._receive_name(socket)
                self.current_tube = tube

    @contextmanager
    def using(self, tube):
        """Context-manager to insert jobs into a specific tube

        :param tube: Tube to insert to

        Yields out an instance of :class:`BeanstalkInsertingProxy` to insert items into that tube

        .. seealso::

           :func:`use()`
              Change the default tube

           :func:`put_job()`
              Put a job into whatever the current tube is

           :func:`put_job_into()`
              Put a job into a specific tube
        """
        try:
            current_tube = self.current_tube
            self.use(tube)
            yield BeanstalkInsertingProxy(self, tube)
        finally:
            self.use(current_tube)

    def kick_jobs(self, num_jobs):
        """Kick some number of jobs from the buried queue onto the ready queue.

        :param num_jobs: Number of jobs to kick
        :type num_jobs: int

        If not that many jobs are in the buried queue, it will kick as many as it can."""
        with self._sock_ctx() as socket:
            self._send_message('kick {0}'.format(num_jobs), socket)
            return self._receive_id(socket)

    def pause_tube(self, tube, delay=3600):
        """Pause a tube for some number of seconds, preventing it from issuing jobs.

        :param delay: Time to pause for, in seconds
        :type delay: int

        There is no way to permanently pause a tube; passing 0 for delay actually un-pauses the tube.

        .. seealso::

           :func:`unpause_tube()`
        """
        with self._sock_ctx() as socket:
            delay = int(delay)
            self._send_message('pause-tube {0} {1}'.format(tube, delay), socket)
            return self._receive_word(socket, b'PAUSED')

    def unpause_tube(self, tube):
        """Unpause a tube which was previously paused with :func:`pause_tube()`.

        .. seealso::

           :func:`pause_tube()`
        """
        with self._sock_ctx() as socket:
            self._send_message('pause-tube {0} 0'.format(tube), socket)
            return self._receive_word(socket, b'PAUSED')
