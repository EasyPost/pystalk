import logging

import pytest

from pystalk.pool import ProductionPool, NoMoreClients
from pystalk.client import BeanstalkError


def test_single_server(beanstalk_client, tube_name):
    p = ProductionPool([beanstalk_client])
    p.put_job_into(tube_name, b'job 1')
    beanstalk_client.watch(tube_name)
    assert beanstalk_client.reserve_job(timeout=1).job_data == b'job 1'


def test_single_broken_server(tube_name, mocker):
    client = mocker.Mock()
    client.current_tube = tube_name
    client.put_job_into.side_effect = BeanstalkError(b'INTERNAL_ERROR')
    p = ProductionPool([client])
    with pytest.raises(NoMoreClients):
        p.put_job_into(tube_name, b'job 1')


def test_one_good_one_bad(tube_name, mocker, beanstalk_client, caplog):
    bad_client = mocker.Mock()
    bad_client.current_tube = tube_name
    bad_client.put_job_into.side_effect = BeanstalkError(b'INTERNAL_ERROR')
    p = ProductionPool([bad_client, beanstalk_client], shuffle=False, backoff_time=10)
    # put a job; this should try the bad tube, then the good tube and succeed
    with caplog.at_level(logging.WARNING):
        p.put_job_into(tube_name, b'job 1')
    assert len(caplog.records) > 0
    beanstalk_client.watch(tube_name)
    assert beanstalk_client.reserve_job(timeout=1).job_data == b'job 1'
    assert bad_client.put_job_into.call_count == 1
    # at this point, the bad tube is in backoff; confirm that it doesn't get hit again
    caplog.clear()
    bad_client.put_job_into.reset_mock()
    with caplog.at_level(logging.WARNING):
        p.put_job_into(tube_name, b'job 2')
    assert len(caplog.records) == 0
    assert beanstalk_client.reserve_job(timeout=1).job_data == b'job 2'
    assert bad_client.put_job_into.call_count == 0


def test_from_uris(beanstalkd, beanstalk_client, tube_name):
    host, port = beanstalkd
    p = ProductionPool.from_uris(['beanstalkd://{0}:{1}'.format(host, port)])
    p.put_job_into(tube_name, b'job 1')
    beanstalk_client.watch(tube_name)
    assert beanstalk_client.reserve_job(timeout=1).job_data == b'job 1'
