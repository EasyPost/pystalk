import pystalk

import pytest


class MockBeanstalkServerSocket(object):
    def __init__(self):
        self.received = []
        self.responses = []

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


def test_connection_error_is_wrapped(monkeypatch):
    def boom(*args, **kwargs):
        raise ConnectionRefusedError('refused')
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
    # original socket error is preserved via __cause__
    assert isinstance(ei.value.__cause__, ConnectionRefusedError)
    assert isinstance(ei.value.err, ConnectionRefusedError)


def test_connection_error_is_a_beanstalk_error(monkeypatch):
    # Success criterion: existing `except BeanstalkError` still catches it
    def boom(*args, **kwargs):
        raise OSError('nope')
    monkeypatch.setattr('pystalk.client.socket.create_connection', boom)
    client = pystalk.BeanstalkClient('h', 1)
    client.socket = None
    with pytest.raises(pystalk.BeanstalkError):   # must still be caught here
        _ = client._socket
