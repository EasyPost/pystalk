import collections
import functools
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


def with_socket(f):
    @functools.wraps(f)
    def inner(self, *args, **kwargs):
        socket = self._connect()
        return f(self, socket, *args, **kwargs)
    return inner


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
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.watchlist = ['default']
        self.current_tube = 'default'
        self.initial_watch = True

    def __repr__(self):
        return '{0}({1!r}, {2!r})'.format(self.__class__.__name__, self.host, self.port)  # pragma: no cover

    def __str__(self):
        return '{0} - watching:{1}, current:{2}'.format(  # pragma: no cover
            repr(self), self.watchlist, self.current_tube  # pragma: no cover
        )  # pragma: no cover

    def _connect(self):
        if self.socket is None:
            self.socket = socket.create_connection((self.host, self.port))
        return self.socket

    def receive_data_with_prefix(self, prefix, sock):
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
        return self.receive_data(sock, rest)

    def receive_id_and_data_with_prefix(self, prefix, sock):
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
        return int(the_id), self.receive_data(sock, rest)

    def receive_data(self, sock, initial=None):
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
        return b''.join(buf)[:-2]

    def receive_id(self, sock):
        status, gid = self.receive_name(sock)
        return status, int(gid)

    def receive_name(self, sock):
        message = sock.recv(1024)
        if b' ' in message:
            status, rest = message.split(b' ', 1)
            return status, rest.rstrip()
        else:
            raise BeanstalkError(message.rstrip())

    def receive_word(self, sock, *expected_words):
        message = sock.recv(1024).rstrip()
        if message not in expected_words:
            raise BeanstalkError(message)
        return message

    def send_message(self, message, socket):
        if isinstance(message, bytes):
            if not message.endswith(b'\r\n'):
                message += b'\r\n'
            return socket.sendall(message)
        else:
            if not message.endswith('\r\n'):
                message += '\r\n'
            return socket.sendall(message.encode('utf-8'))

    @with_socket
    def list_tubes(self, socket):
        self.send_message('list-tubes', socket)
        body = self.receive_data_with_prefix(b'OK', socket)
        tubes = yaml_load(body)
        return tubes

    @with_socket
    def stats(self, socket):
        self.send_message('stats', socket)
        body = self.receive_data_with_prefix(b'OK', socket)
        stats = yaml_load(body)
        return stats

    @with_socket
    def put_job(self, socket, data, pri=65536, delay=0, ttr=120):
        message = 'put {pri} {delay} {ttr} {datalen}\r\n'.format(
            pri=pri, delay=delay, ttr=ttr, datalen=len(data), data=data
        ).encode('utf-8')
        if not isinstance(data, bytes):
            data = data.encode('utf-8')
        message += data
        message += b'\r\n'
        self.send_message(message, socket)
        return self.receive_id(socket)

    @with_socket
    def watch(self, socket, tube):
        self.send_message('watch {0}'.format(tube), socket)
        self.receive_id(socket)
        if self.initial_watch:
            if tube != 'default':
                self.ignore('default')
            self.initial_watch = False
        self.watchlist.append(tube)

    @with_socket
    def ignore(self, socket, tube):
        if tube not in self.watchlist:
            raise KeyError(tube)
        self.send_message('ignore {0}'.format(tube), socket)
        self.receive_id(socket)
        self.watchlist.remove(tube)

    @with_socket
    def stats_job(self, socket, job_id):
        self.send_message('stats-job {0}'.format(job_id), socket)
        body = self.receive_data_with_prefix(b'OK', socket)
        job_status = yaml_load(body)
        return job_status

    @with_socket
    def stats_tube(self, socket, tube_name):
        self.send_message('stats-tube {0}'.format(tube_name), socket)
        body = self.receive_data_with_prefix(b'OK', socket)
        return yaml_load(body)

    @with_socket
    def reserve_job(self, socket, timeout=5):
        if not self.watchlist:
            raise ValueError('Select a tube or two before reserving a job')
        self.send_message('reserve-with-timeout {0}'.format(timeout), socket)
        job_id, job_data = self.receive_id_and_data_with_prefix(b'RESERVED', socket)
        return Job(job_id, job_data)

    @with_socket
    def peek_delayed(self, socket):
        self.send_message('peek-delayed', socket)
        job_id, job_data = self.receive_id_and_data_with_prefix(b'FOUND', socket)
        return Job(job_id, job_data)

    @with_socket
    def peek_buried(self, socket):
        self.send_message('peek-buried', socket)
        job_id, job_data = self.receive_id_and_data_with_prefix(b'FOUND', socket)
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
        """Peek at jobs in sequence"""
        return self._common_iter(self.peek_delayed, 'NOT_FOUND')

    def peek_buried_iter(self):
        """Peek at jobs in sequence"""
        return self._common_iter(self.peek_buried, 'NOT_FOUND')

    @with_socket
    def delete_job(self, socket, job_id):
        self.send_message('delete {0}'.format(job_id), socket)
        self.receive_word(socket, b'DELETED')

    @with_socket
    def bury_job(self, socket, job_id, pri=65536):
        self.send_message('bury {0} {1}'.format(job_id, pri), socket)
        return self.receive_word(socket, b'BURIED')

    @with_socket
    def release_job(self, socket, job_id, pri=65536, delay=0):
        self.send_message('release {0} {1} {2}\r\n'.format(job_id, pri, delay), socket)
        return self.receive_word(socket, b'RELEASED', b'BURIED')

    @with_socket
    def use(self, socket, tube_name):
        """Start producing jobs into the given tube"""
        self.send_message('use {0}'.format(tube_name), socket)
        self.receive_name(socket)
        self.current_tube = tube_name

    @with_socket
    def kick_jobs(self, socket, num_jobs):
        self.send_message('kick {0}'.format(num_jobs), socket)
        return self.receive_id(socket)

    @with_socket
    def pause_tube(self, socket, tube, delay=3600):
        delay = int(delay)
        self.send_message('pause-tube {0} {1}'.format(tube, delay), socket)
        return self.receive_word(socket, b'PAUSED')

    @with_socket
    def unpause_tube(self, socket, tube):
        self.send_message('pause-tube {0} 0'.format(tube), socket)
        return self.receive_word(socket, b'PAUSED')
