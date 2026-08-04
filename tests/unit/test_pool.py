import socket
from unittest.mock import Mock

import pytest

from pystalk import BeanstalkConnectionError
from pystalk import pool
from pystalk.pool import ClientRecord, NoMoreClients, ProductionPool


def make_client(name, calls, result=None, error=None):
    client = Mock(name=name)
    client.current_tube = 'default'

    def put_job(**kwargs):
        calls.append(name)
        if error is not None:
            raise error
        return result

    client.put_job.side_effect = put_job
    return client


def test_client_record_recovers_at_backoff_deadline():
    client_record = ClientRecord(Mock())
    client_record.mark_failed(now=5)

    assert client_record.is_ok(backoff_time=10, now=14.999) is False
    assert client_record.is_ok(backoff_time=10, now=15) is True


def test_connection_error_fails_over_to_next_client():
    calls = []
    connection_error = BeanstalkConnectionError('bad', 11300, ConnectionRefusedError('refused'))
    bad_client = make_client('bad', calls, error=connection_error)
    good_client = make_client('good', calls, result=123)
    production_pool = ProductionPool([bad_client, good_client], initial_shuffle=False, round_robin=False)

    assert production_pool.put_job(b'job') == 123
    assert calls == ['bad', 'good']
    bad_client.close.assert_called_once_with()


def test_all_clients_are_attempted_once_with_zero_backoff():
    calls = []
    first_client = make_client('first', calls, error=socket.error('disconnected'))
    second_client = make_client('second', calls, error=socket.error('disconnected'))
    production_pool = ProductionPool(
        [first_client, second_client], initial_shuffle=False, round_robin=False, backoff_time=0,
    )

    with pytest.raises(NoMoreClients):
        production_pool.put_job(b'job')

    assert calls == ['first', 'second']
    first_client.close.assert_called_once_with()
    second_client.close.assert_called_once_with()


def test_all_down_clients_are_not_retried_until_backoff_expires(monkeypatch):
    now = [0]
    monkeypatch.setattr(pool, '_get_time', lambda: now[0])
    calls = []
    first_client = make_client('first', calls)

    def first_put_job(**kwargs):
        calls.append('first')
        if first_client.put_job.call_count == 1:
            raise socket.error('disconnected')
        return 123

    first_client.put_job.side_effect = first_put_job
    second_client = make_client('second', calls, error=socket.error('disconnected'))
    production_pool = ProductionPool(
        [first_client, second_client], initial_shuffle=False, round_robin=True, backoff_time=10,
    )

    with pytest.raises(NoMoreClients):
        production_pool.put_job(b'first')
    assert calls == ['first', 'second']

    now[0] = 9.999
    with pytest.raises(NoMoreClients):
        production_pool.put_job(b'during-backoff')
    assert calls == ['first', 'second']

    now[0] = 10
    assert production_pool.put_job(b'after-backoff') == 123
    assert calls == ['first', 'second', 'first']


def test_tube_activation_failure_fails_over_to_correct_client():
    calls = []
    bad_client = make_client('bad', calls)
    bad_client.use.side_effect = socket.error('disconnected')
    good_client = make_client('good', calls, result=123)
    production_pool = ProductionPool([bad_client, good_client], initial_shuffle=False, round_robin=False)
    production_pool.use('jobs')

    assert production_pool.put_job(b'job') == 123
    assert calls == ['good']
    bad_client.close.assert_called_once_with()
    good_client.use.assert_called_once_with('jobs')


def test_round_robin_uses_clients_in_declared_order():
    calls = []
    clients = [make_client(name, calls) for name in ('first', 'second', 'third')]
    production_pool = ProductionPool(clients, initial_shuffle=False, round_robin=True)

    for _ in clients:
        production_pool.put_job(b'job')

    assert calls == ['first', 'second', 'third']


def test_flapping_client_is_retried_after_backoff(monkeypatch):
    now = [0]
    monkeypatch.setattr(pool, '_get_time', lambda: now[0])
    calls = []
    flapping_client = make_client('flapping', calls)
    flapping_client.put_job.side_effect = [socket.error('disconnected'), 456]
    good_client = make_client('good', calls, result=123)
    production_pool = ProductionPool(
        [flapping_client, good_client], initial_shuffle=False, round_robin=True, backoff_time=10,
    )

    assert production_pool.put_job(b'first') == 123
    now[0] = 9.999
    assert production_pool.put_job(b'during-backoff') == 123
    assert flapping_client.put_job.call_count == 1

    now[0] = 10
    assert production_pool.put_job(b'after-backoff') == 456

    assert flapping_client.put_job.call_count == 2
    flapping_client.close.assert_called_once_with()
