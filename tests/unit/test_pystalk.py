import pystalk

import pytest
import socket
from unittest.mock import Mock


class MockBeanstalkServerSocket(object):
    def __init__(self):
        self.closed = False
        self.received = []
        self.responses = []
        self.timeout = None
        self.settimeout_calls = []

    def close(self):
        self.closed = True

    def settimeout(self, timeout):
        self.timeout = timeout
        self.settimeout_calls.append(timeout)

    def sendall(self, bytez):
        if not isinstance(bytez, bytes):
            raise TypeError("Expected bytes!")
        self.received.append(bytez)

    def recv(self, size):
        resp = self.responses.pop()
        return resp


@pytest.fixture
def server():
    return MockBeanstalkServerSocket()


@pytest.fixture
def client(server):
    client = pystalk.BeanstalkClient('pystalk.example.com', 0)
    client.socket = server
    return client


def test_stats(client, server):
    server.responses.append(b'OK 17\r\n--- {"foo": "bar"}\r\n')
    assert client.stats() == {'foo': 'bar'}
    assert server.received == [b'stats\r\n']


def test_reserve_raises_connection_error_when_server_closes_connection(client, server):
    client.current_tube = 'jobs'
    client.desired_tube = 'jobs'
    client._watchlist = {'jobs'}
    client.desired_watchlist = {'default', 'jobs'}
    client.initial_watch = False
    server.responses.append(b'')

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.reserve_job(0)

    assert isinstance(exc_info.value.err, ConnectionResetError)
    assert exc_info.value.__cause__ is exc_info.value.err
    assert exc_info.value.host == 'pystalk.example.com'
    assert exc_info.value.port == 0
    assert server.closed is True
    assert client.socket is None
    assert client.current_tube == 'default'
    assert client._watchlist == {'default'}
    assert client.initial_watch is True
    assert client.desired_tube == 'jobs'
    assert client.desired_watchlist == {'default', 'jobs'}


def test_put_job_uses_utf8_byte_length_for_non_ascii(client, server):
    server.responses.append(b'INSERTED 1\r\n')

    status, job_id = client.put_job('caf\u00e9 (coffee)')

    assert (status, job_id) == (b'INSERTED', 1)
    assert server.received == [b'put 65536 0 120 14\r\ncaf\xc3\xa9 (coffee)\r\n']


@pytest.mark.parametrize('uri,expected_host,expected_port', [
    ('beanstalkd://foo', 'foo', 11300),
    ('beanstalk://foo', 'foo', 11300),
    ('beanstalk://foo:11301', 'foo', 11301),
    ('beanstalk://1.2.3.4', '1.2.3.4', 11300),
    ('beanstalk://1.2.3.4:11301', '1.2.3.4', 11301),
    ('beanstalk://[::1]', '::1', 11300),
    ('beanstalk://[::1]:11301', '::1', 11301),
])
def test_from_uri(uri, expected_host, expected_port):
    client = pystalk.BeanstalkClient.from_uri(uri)
    assert client.host == expected_host
    assert client.port == expected_port


@pytest.mark.parametrize('uri', [
    'branstalk://foo:12345',
    'beanstalk://foo:bar',
])
def test_invalid_uri_fails(uri):
    with pytest.raises(ValueError):
        pystalk.BeanstalkClient.from_uri(uri)


def test_ping_connects_without_mutating_client_state(monkeypatch, client, server):
    ping_socket = MockBeanstalkServerSocket()
    monkeypatch.setattr('pystalk.client.socket.create_connection', lambda *args, **kwargs: ping_socket)
    client.current_tube = 'jobs'
    client._watchlist = {'jobs', 'urgent'}

    assert client.ping() is True

    assert ping_socket.closed is True
    assert client.socket is server
    assert client.current_tube == 'jobs'
    assert client._watchlist == {'jobs', 'urgent'}


def test_ping_on_fresh_client_does_not_install_probe_socket(monkeypatch):
    ping_socket = MockBeanstalkServerSocket()
    create_connection = Mock(return_value=ping_socket)
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)
    client = pystalk.BeanstalkClient('pystalk.example.com', 11301, socket_timeout=2.5)

    assert client.ping() is True

    create_connection.assert_called_once_with(('pystalk.example.com', 11301), timeout=2.5)
    assert ping_socket.closed is True
    assert client.socket is None


@pytest.mark.parametrize('connection_error', [
    ConnectionRefusedError('refused'),
    socket.timeout('timed out'),
    socket.error('socket error'),
    socket.gaierror(-2, 'Name or service not known'),
])
def test_ping_raises_connection_error_for_unreachable_server(monkeypatch, connection_error):

    def refuse_connection(*args, **kwargs):
        raise connection_error

    monkeypatch.setattr('pystalk.client.socket.create_connection', refuse_connection)
    client = pystalk.BeanstalkClient('unreachable.example.com', 11300)

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.ping()

    assert exc_info.value.host == 'unreachable.example.com'
    assert exc_info.value.port == 11300
    assert exc_info.value.err is connection_error
    assert exc_info.value.__context__ is connection_error
    assert client.socket is None
    assert client.current_tube == 'default'
    assert client._watchlist == {'default'}


@pytest.mark.parametrize('raised_error', [
    ConnectionRefusedError('refused'),
    socket.timeout('timed out'),
    socket.error('socket error'),
    socket.gaierror(-2, 'Name or service not known'),
])
def test_connection_error_is_wrapped(monkeypatch, raised_error):
    def boom(*args, **kwargs):
        raise raised_error
    monkeypatch.setattr('pystalk.client.socket.create_connection', boom)

    client = pystalk.BeanstalkClient('pystalk.example.com', 11300)
    # ensure fresh (no injected mock socket)
    client.socket = None

    with pytest.raises(pystalk.BeanstalkConnectionError) as ei:
        _ = client._socket
    assert 'pystalk.example.com' in str(ei.value)
    assert '11300' in str(ei.value)
    # host/port context is exposed on the exception
    assert ei.value.host == 'pystalk.example.com'
    assert ei.value.port == 11300
    # original socket error is preserved via implicit context chaining
    assert ei.value.__context__ is raised_error
    assert ei.value.err is raised_error


def test_connection_error_is_a_beanstalk_error():
    connection_error = pystalk.BeanstalkConnectionError('host', 11300, ConnectionRefusedError('refused'))

    with pytest.raises(pystalk.BeanstalkError) as exc_info:
        raise connection_error

    assert exc_info.value is connection_error


@pytest.mark.parametrize('raised_error', [
    ConnectionResetError(104, 'Connection reset by peer'),
    BrokenPipeError(32, 'Broken pipe'),
    socket.timeout('timed out'),
    TimeoutError('timed out'),
    OSError('socket failure'),
])
def test_sendall_oserror_raises_connection_error_and_resets_socket(client, server, raised_error):
    client.current_tube = 'jobs'
    client.desired_tube = 'jobs'
    client._watchlist = {'jobs'}
    client.desired_watchlist = {'default', 'jobs'}
    client.initial_watch = False
    server.sendall = Mock(side_effect=raised_error)

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.put_job('hello')

    assert exc_info.value.host == 'pystalk.example.com'
    assert exc_info.value.port == 0
    assert exc_info.value.err is raised_error
    assert exc_info.value.__cause__ is raised_error
    assert server.closed is True
    assert client.socket is None
    assert client.current_tube == 'default'
    assert client._watchlist == {'default'}
    assert client.initial_watch is True
    assert client.desired_tube == 'jobs'
    assert client.desired_watchlist == {'default', 'jobs'}


@pytest.mark.parametrize('raised_error', [
    ConnectionResetError(104, 'Connection reset by peer'),
    BrokenPipeError(32, 'Broken pipe'),
    socket.timeout('timed out'),
    TimeoutError('timed out'),
    OSError('socket failure'),
])
def test_recv_oserror_raises_connection_error_and_resets_socket(client, server, raised_error):
    client.current_tube = 'jobs'
    client.desired_tube = 'jobs'
    client._watchlist = {'jobs'}
    client.desired_watchlist = {'default', 'jobs'}
    client.initial_watch = False
    server.recv = Mock(side_effect=raised_error)

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.reserve_job(0)

    assert exc_info.value.host == 'pystalk.example.com'
    assert exc_info.value.port == 0
    assert exc_info.value.err is raised_error
    assert exc_info.value.__cause__ is raised_error
    assert server.closed is True
    assert client.socket is None
    assert client.current_tube == 'default'
    assert client._watchlist == {'default'}
    assert client.initial_watch is True
    assert client.desired_tube == 'jobs'
    assert client.desired_watchlist == {'default', 'jobs'}


def test_close_suppresses_socket_close_errors_and_resets_state(client, server):
    client.current_tube = 'jobs'
    client._watchlist = {'jobs'}
    client.initial_watch = False
    server.close = Mock(side_effect=OSError('bad file descriptor'))

    client.close()

    server.close.assert_called_once_with()
    assert client.socket is None
    assert client.current_tube == 'default'
    assert client._watchlist == {'default'}
    assert client.initial_watch is True


def test_error_during_socket_close_does_not_mask_connection_error(client, server):
    server.close = Mock(side_effect=OSError('bad file descriptor'))
    reset_error = ConnectionResetError(104, 'Connection reset by peer')
    server.sendall = Mock(side_effect=reset_error)

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.stats()

    assert exc_info.value.err is reset_error
    assert exc_info.value.__cause__ is reset_error
    server.close.assert_called_once_with()
    assert client.socket is None
    assert client.current_tube == 'default'
    assert client._watchlist == {'default'}


@pytest.mark.parametrize('failure_mode', ['sendall', 'recv', 'eof'])
def test_next_call_after_connection_error_reconnects_and_restores_use_watch(monkeypatch, failure_mode):
    first_socket = MockBeanstalkServerSocket()
    # Initial setup responses (popped in LIFO order): watch work -> ignore default, then use jobs
    first_socket.responses = [
        b'USING jobs\r\n',
        b'WATCHING 1\r\n',
        b'WATCHING 2\r\n',
    ]
    client = pystalk.BeanstalkClient('pystalk.example.com', 11300)
    client.socket = first_socket
    client.watch('work')
    client.use('jobs')

    if failure_mode == 'sendall':
        first_socket.sendall = Mock(side_effect=ConnectionResetError(104, 'Connection reset by peer'))
    elif failure_mode == 'recv':
        first_socket.recv = Mock(side_effect=ConnectionResetError(104, 'Connection reset by peer'))
    else:
        first_socket.responses = [b'']

    with pytest.raises(pystalk.BeanstalkConnectionError):
        client.put_job('lost')

    assert first_socket.closed is True
    assert client.socket is None

    second_socket = MockBeanstalkServerSocket()
    # Reconnect responses (LIFO): use jobs, watch work, ignore default, put_job, reserve_job
    second_socket.responses = [
        b'RESERVED 99 5\r\nsaved\r\n',
        b'INSERTED 99\r\n',
        b'WATCHING 1\r\n',
        b'WATCHING 2\r\n',
        b'USING jobs\r\n',
    ]
    create_connection = Mock(return_value=second_socket)
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)

    assert client.put_job('saved') == (b'INSERTED', 99)
    job = client.reserve_job(0)
    assert job.job_id == 99
    assert job.job_data == b'saved'

    create_connection.assert_called_once_with(('pystalk.example.com', 11300), timeout=None)
    assert client.socket is second_socket
    assert second_socket.received == [
        b'use jobs\r\n',
        b'watch work\r\n',
        b'ignore default\r\n',
        b'put 65536 0 120 5\r\nsaved\r\n',
        b'reserve-with-timeout 0\r\n',
    ]


def test_failed_reconnect_during_use_watch_restore_closes_half_open_socket(monkeypatch):
    broken_reconnect_socket = MockBeanstalkServerSocket()
    broken_reconnect_socket.recv = Mock(side_effect=ConnectionResetError(104, 'Connection reset by peer'))

    healthy_socket = MockBeanstalkServerSocket()
    healthy_socket.responses = [
        b'INSERTED 7\r\n',
        b'USING jobs\r\n',
    ]

    create_connection = Mock(side_effect=[broken_reconnect_socket, healthy_socket])
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)

    client = pystalk.BeanstalkClient('pystalk.example.com', 11300)
    client.desired_tube = 'jobs'

    with pytest.raises(pystalk.BeanstalkConnectionError):
        client.put_job('first')

    assert broken_reconnect_socket.closed is True
    assert client.socket is None

    assert client.put_job('second') == (b'INSERTED', 7)
    assert client.socket is healthy_socket
    assert create_connection.call_count == 2


def test_using_does_not_reconnect_in_finally_after_connection_error(monkeypatch, client, server):
    server.responses = [b'USING jobs\r\n']
    client.use('jobs')

    reset_error = ConnectionResetError(104, 'Connection reset by peer')
    server.responses = [b'USING temp\r\n']
    orig_sendall = server.sendall

    def fail_on_put(bytez):
        if bytez.startswith(b'put '):
            raise reset_error
        return orig_sendall(bytez)

    server.sendall = fail_on_put
    create_connection = Mock(side_effect=AssertionError('should not reconnect in finally'))
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.put_job_into('temp', 'payload')

    assert exc_info.value.err is reset_error
    assert client.socket is None
    assert client.desired_tube == 'jobs'
    create_connection.assert_not_called()


def test_connect_uses_connect_timeout_then_applies_socket_timeout(monkeypatch):
    sock = MockBeanstalkServerSocket()
    sock.responses = [b'OK 17\r\n--- {"foo": "bar"}\r\n']
    observed_connect_timeout = []

    def fake_create_connection(address, timeout=None):
        observed_connect_timeout.append((address, timeout))
        sock.timeout = timeout
        return sock

    monkeypatch.setattr('pystalk.client.socket.create_connection', fake_create_connection)
    client = pystalk.BeanstalkClient('pystalk.example.com', 11300, socket_timeout=30.0, connect_timeout=1.5)

    assert client.stats() == {'foo': 'bar'}

    assert observed_connect_timeout == [(('pystalk.example.com', 11300), 1.5)]
    assert sock.settimeout_calls == [30.0]
    assert sock.timeout == 30.0


def test_connect_with_connect_timeout_and_none_socket_timeout_clears_timeout(monkeypatch):
    sock = MockBeanstalkServerSocket()
    sock.responses = [b'OK 17\r\n--- {"foo": "bar"}\r\n']

    def fake_create_connection(address, timeout=None):
        sock.timeout = timeout
        return sock

    create_connection = Mock(side_effect=fake_create_connection)
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)
    client = pystalk.BeanstalkClient('pystalk.example.com', 11300, socket_timeout=None, connect_timeout=2.0)

    assert client.stats() == {'foo': 'bar'}

    create_connection.assert_called_once_with(('pystalk.example.com', 11300), timeout=2.0)
    assert sock.settimeout_calls == [None]
    assert sock.timeout is None


def test_connect_falls_back_to_socket_timeout_when_connect_timeout_unset(monkeypatch):
    sock = MockBeanstalkServerSocket()
    sock.responses = [b'OK 17\r\n--- {"foo": "bar"}\r\n']
    create_connection = Mock(return_value=sock)
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)
    client = pystalk.BeanstalkClient('pystalk.example.com', 11300, socket_timeout=12.0)

    assert client.stats() == {'foo': 'bar'}

    create_connection.assert_called_once_with(('pystalk.example.com', 11300), timeout=12.0)
    assert sock.settimeout_calls == [12.0]
    assert sock.timeout == 12.0


def test_ping_uses_connect_timeout_when_set(monkeypatch):
    ping_socket = MockBeanstalkServerSocket()
    create_connection = Mock(return_value=ping_socket)
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)
    client = pystalk.BeanstalkClient('pystalk.example.com', 11301, socket_timeout=30.0, connect_timeout=1.25)

    assert client.ping() is True

    create_connection.assert_called_once_with(('pystalk.example.com', 11301), timeout=1.25)
    assert ping_socket.closed is True
    assert client.socket is None


def test_from_uri_passes_connect_timeout(monkeypatch):
    sock = MockBeanstalkServerSocket()
    sock.responses = [b'OK 17\r\n--- {"foo": "bar"}\r\n']
    create_connection = Mock(return_value=sock)
    monkeypatch.setattr('pystalk.client.socket.create_connection', create_connection)

    client = pystalk.BeanstalkClient.from_uri(
        'beanstalkd://pystalk.example.com:11301',
        socket_timeout=25.0,
        auto_decode=True,
        connect_timeout=2.0,
    )

    assert client.host == 'pystalk.example.com'
    assert client.port == 11301
    assert client.socket_timeout == 25.0
    assert client.auto_decode is True
    assert client.connect_timeout == 2.0

    assert client.stats() == {'foo': 'bar'}
    create_connection.assert_called_once_with(('pystalk.example.com', 11301), timeout=2.0)
    assert sock.settimeout_calls == [25.0]
