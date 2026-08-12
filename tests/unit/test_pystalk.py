import pystalk

import pytest
import socket
from unittest.mock import Mock


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


def test_reserve_raises_connection_error_when_server_closes_connection(client, server):
    server.responses.append(b'')

    with pytest.raises(pystalk.BeanstalkConnectionError) as exc_info:
        client.reserve_job(0)

    assert isinstance(exc_info.value.err, ConnectionResetError)
    assert exc_info.value.host == 'pystalk.example.com'
    assert exc_info.value.port == 0


def test_put_job_uses_utf8_byte_length_for_non_ascii(client, server):
    server.responses.append(b'INSERTED 1\r\n')

    status, job_id = client.put_job('caf\u00e9 (coffee)')

    assert (status, job_id) == (b'INSERTED', 1)
    assert server.received == [b'put 65536 0 120 14\r\ncaf\xc3\xa9 (coffee)\r\n']


def test_put_job_with_reference_qualifies_job_id_with_server(client, server):
    server.responses.append(b'INSERTED 42\r\n')

    reference = client.put_job_with_reference('job')

    assert reference == pystalk.JobReference('pystalk.example.com:0', 42)
    assert server.received == [b'put 65536 0 120 3\r\njob\r\n']


def test_put_job_into_with_reference_uses_selected_tube(client, monkeypatch):
    client.current_tube = 'jobs'
    put_job = Mock(return_value=(b'INSERTED', 42))
    monkeypatch.setattr(client, 'put_job', put_job)

    reference = client.put_job_into_with_reference('jobs', 'job', pri=10, delay=20, ttr=30)

    assert reference == pystalk.JobReference('pystalk.example.com:0', 42)
    put_job.assert_called_once_with(data='job', pri=10, delay=20, ttr=30)


def test_job_reference_round_trips_through_dictionary():
    reference = pystalk.JobReference('primary-beanstalk', 42)

    assert pystalk.JobReference.from_dict(reference.to_dict()) == reference


@pytest.mark.parametrize(
    'value, exception_type, message',
    [
        (None, TypeError, 'Job reference must be a dictionary'),
        ({'server_id': 'primary-beanstalk'}, ValueError, 'Job reference is missing job_id'),
    ]
)
def test_job_reference_rejects_invalid_dictionary(value, exception_type, message):
    with pytest.raises(exception_type, match=message):
        pystalk.JobReference.from_dict(value)


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


def test_client_accepts_stable_server_id_for_job_references():
    client = pystalk.BeanstalkClient.from_uri(
        'beanstalk://alias.example.com:11300',
        server_id='primary-beanstalk',
    )

    assert client.server_id == 'primary-beanstalk'


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
