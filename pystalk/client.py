from contextlib import contextmanager
import collections
import socket
import yaml


Job = collections.namedtuple('Job', ['job_id', 'job_data'])


class BeanstalkError(Exception):
    def __init__(self, message):
        self.message = message.decode('ascii')

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.message)  # pragma: no cover

    def __str__(self):
        return repr(self)  # pragma: no cover


def yaml_load(fo):
    # yaml.safe_load will never use the C loader; we have to detect it ourselves
    if hasattr(yaml, 'CSafeLoader'):
        return yaml.load(fo, Loader=yaml.CSafeLoader)
    else:
        return yaml.safe_load(fo)


class BeanstalkClient(object):
    """Simple wrapper around the Beanstalk API.

    Doesn't provide any fanciness for writing consumers or producers. Just lets you invoke methods to call beanstalk
    functions.
    """
    def __init__(self, host, port=11300, socket_timeout=None, auto_decode=False):
        """Construct a synchronous Beanstalk Client. Does not connect!

        :param host: Hostname or IP address to connect to
        :param port: Port to connect to
        :param socket_timeout: Timeout to set on the socket.
        :param auto_decode: Attempt to decode job bodies as UTF-8 when reading them

        NOTE: Setting the socket timeout to a value lower than the value you pass to blocking functions like
        `reserve_job` will cause errors!
        """
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.socket = None
        self.watchlist = ['default']
        self.current_tube = 'default'
        self.initial_watch = True
        self.auto_decode = auto_decode

    def __repr__(self):
        return '{0}({1!r}, {2!r})'.format(self.__class__.__name__, self.host, self.port)  # pragma: no cover

    def __str__(self):
        return '{0} - watching:{1}, current:{2}'.format(  # pragma: no cover
            repr(self), self.watchlist, self.current_tube  # pragma: no cover
        )  # pragma: no cover

    @property
    def _socket(self):
        if self.socket is None:
            self.socket = socket.create_connection((self.host, self.port), timeout=self.socket_timeout)
        return self.socket

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

    def send_message(self, message, sock):
        if isinstance(message, bytes):
            if not message.endswith(b'\r\n'):
                message += b'\r\n'
            return sock.sendall(message)
        else:
            if not message.endswith('\r\n'):
                message += '\r\n'
            return sock.sendall(message.encode('utf-8'))

    def list_tubes(self):
        with self._sock_ctx() as sock:
            self.send_message('list-tubes', sock)
            body = self._receive_data_with_prefix(b'OK', sock)
            tubes = yaml_load(body)
            return tubes

    def stats(self):
        with self._sock_ctx() as socket:
            self.send_message('stats', socket)
            body = self._receive_data_with_prefix(b'OK', socket)
            stats = yaml_load(body)
            return stats

    def put_job(self, data, pri=65536, delay=0, ttr=120):
        with self._sock_ctx() as socket:
            message = 'put {pri} {delay} {ttr} {datalen}\r\n'.format(
                pri=pri, delay=delay, ttr=ttr, datalen=len(data), data=data
            ).encode('utf-8')
            if not isinstance(data, bytes):
                data = data.encode('utf-8')
            message += data
            message += b'\r\n'
            self.send_message(message, socket)
            return self._receive_id(socket)

    def watch(self, tube):
        with self._sock_ctx() as socket:
            self.send_message('watch {0}'.format(tube), socket)
            self._receive_id(socket)
            if self.initial_watch:
                if tube != 'default':
                    self.ignore('default')
                self.initial_watch = False
            self.watchlist.append(tube)

    def ignore(self, tube):
        with self._sock_ctx() as socket:
            if tube not in self.watchlist:
                raise KeyError(tube)
            self.send_message('ignore {0}'.format(tube), socket)
            self._receive_id(socket)
            self.watchlist.remove(tube)

    def stats_job(self, job_id):
        with self._sock_ctx() as socket:
            if hasattr(job_id, 'job_id'):
                job_id = job_id.job_id
            self.send_message('stats-job {0}'.format(job_id), socket)
            body = self._receive_data_with_prefix(b'OK', socket)
            job_status = yaml_load(body)
            return job_status

    def stats_tube(self, tube_name):
        with self._sock_ctx() as socket:
            self.send_message('stats-tube {0}'.format(tube_name), socket)
            body = self._receive_data_with_prefix(b'OK', socket)
            return yaml_load(body)

    def reserve_job(self, timeout=5):
        """Reserve a job for this connection. Blocks for TIMEOUT secionds and raises TIMED_OUT if no job was available

        :param timeout: Time to wait for a job, in seconds. Must be an integer.
        """
        timeout = int(timeout)
        if not self.watchlist:
            raise ValueError('Select a tube or two before reserving a job')
        with self._sock_ctx() as socket:
            self.send_message('reserve-with-timeout {0}'.format(timeout), socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'RESERVED', socket)
            return Job(job_id, job_data)

    def peek_ready(self):
        """Peek at the job job on the ready queue"""
        with self._sock_ctx() as socket:
            self.send_message('peek-ready', socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'FOUND', socket)
            return Job(job_id, job_data)

    def peek_delayed(self):
        """Peek at the job job on the delayed queue"""
        with self._sock_ctx() as socket:
            self.send_message('peek-delayed', socket)
            job_id, job_data = self._receive_id_and_data_with_prefix(b'FOUND', socket)
            return Job(job_id, job_data)

    def peek_buried(self):
        """Peek at the top job on the buried queue"""
        with self._sock_ctx() as socket:
            self.send_message('peek-buried', socket)
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
            self.send_message('delete {0}'.format(job_id), socket)
            self._receive_word(socket, b'DELETED')

    def bury_job(self, job_id, pri=65536):
        """Mark the given job_id as buried. The job must have been previously reserved by this connection"""
        if hasattr(job_id, 'job_id'):
            job_id = job_id.job_id
        with self._sock_ctx() as socket:
            self.send_message('bury {0} {1}'.format(job_id, pri), socket)
            return self._receive_word(socket, b'BURIED')

    def release_job(self, job_id, pri=65536, delay=0):
        if hasattr(job_id, 'job_id'):
            job_id = job_id.job_id
        with self._sock_ctx() as socket:
            self.send_message('release {0} {1} {2}\r\n'.format(job_id, pri, delay), socket)
            return self._receive_word(socket, b'RELEASED', b'BURIED')

    def use(self, tube_name):
        """Start producing jobs into the given tube. Subsequent calls to .put_job will go here!"""
        with self._sock_ctx() as socket:
            self.send_message('use {0}'.format(tube_name), socket)
            self._receive_name(socket)
            self.current_tube = tube_name

    def kick_jobs(self, num_jobs):
        """Kick some number of jobs from the buried queue onto the ready queue"""
        with self._sock_ctx() as socket:
            self.send_message('kick {0}'.format(num_jobs), socket)
            return self._receive_id(socket)

    def pause_tube(self, tube, delay=3600):
        with self._sock_ctx() as socket:
            delay = int(delay)
            self.send_message('pause-tube {0} {1}'.format(tube, delay), socket)
            return self._receive_word(socket, b'PAUSED')

    def unpause_tube(self, tube):
        with self._sock_ctx() as socket:
            self.send_message('pause-tube {0} 0'.format(tube), socket)
            return self._receive_word(socket, b'PAUSED')
