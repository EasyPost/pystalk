import pystalk

import pytest
import socket


class MockBeanstalkServerSocket(object):
    def __init__(self):
        self.closed = False
        self.received = []
        self.responses = []

    def close(self):
        self.closed = True

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


def test_ping_raises_connection_error_for_unreachable_server(monkeypatch):
    connection_error = ConnectionRefusedError('refused')

    def refuse_connection(*args, **kwargs):
        raise connection_error

    monkeypatch.setattr('pystalk.client.socket.create_connection', refuse_connection)
    client = pystalk.BeanstalkClient('unreachable.example.com', 11300)

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.ping()

    assert exc_info.value.err is connection_error
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


def test_connection_error_is_a_beanstalk_error(monkeypatch):
    # Success criterion: existing `except BeanstalkError` still catches it
    def boom(*args, **kwargs):
        raise pystalk.BeanstalkError('nope')
    monkeypatch.setattr('pystalk.client.socket.create_connection', boom)
    client = pystalk.BeanstalkClient('h', 1)
    client.socket = None
    with pytest.raises(pystalk.BeanstalkError):
        _ = client._socket
