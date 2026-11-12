import pytest

from pystalk import ProductionPool, connect_beanstalk, parse_beanstalk_urls
from pystalk.helpers import connect_beanstalk as helpers_connect_beanstalk


def test_parse_string_input():
    assert parse_beanstalk_urls('host1:11300,host2:11300') == [
        'beanstalk://host1:11300',
        'beanstalk://host2:11300',
    ]


def test_parse_list_input():
    assert parse_beanstalk_urls(['host1:11300', 'host2:11300']) == [
        'beanstalk://host1:11300',
        'beanstalk://host2:11300',
    ]


def test_parse_strips_whitespace_and_quotes():
    assert parse_beanstalk_urls('  "host1:11300" ,\t\'host2:11300\'\r\n') == [
        'beanstalk://host1:11300',
        'beanstalk://host2:11300',
    ]


def test_parse_drops_empty_entries():
    assert parse_beanstalk_urls(',host1:11300,, " ",') == ['beanstalk://host1:11300']
    assert parse_beanstalk_urls(['', '  ']) == []


def test_parse_prepends_scheme_when_missing():
    assert parse_beanstalk_urls('host1') == ['beanstalk://host1']


def test_parse_preserves_schemes():
    assert parse_beanstalk_urls(['beanstalk://host1:11300', 'beanstalkd://host2:11300']) == [
        'beanstalk://host1:11300',
        'beanstalkd://host2:11300',
    ]


def test_parse_ipv6_literal():
    assert parse_beanstalk_urls('[::1]:11300') == ['beanstalk://[::1]:11300']


def test_parse_bad_scheme_raises_value_error():
    with pytest.raises(ValueError, match='Invalid scheme'):
        ProductionPool.from_uris(parse_beanstalk_urls('http://host1:11300'))


def test_connect_reads_env_var():
    environ = {'BEANSTALKD_URLS': 'host1:11300,host2:11300'}
    pool = connect_beanstalk(environ=environ, initial_shuffle=False)
    assert [record.client.host for record in pool._clients] == ['host1', 'host2']


def test_connect_custom_env_var():
    environ = {'MY_URLS': 'host1:11300'}
    pool = connect_beanstalk(env_var='MY_URLS', environ=environ)
    assert [record.client.host for record in pool._clients] == ['host1']


def test_connect_missing_env_var_raises_value_error():
    with pytest.raises(ValueError, match='BEANSTALKD_URLS'):
        connect_beanstalk(environ={})


def test_connect_empty_env_var_raises_value_error():
    with pytest.raises(ValueError, match='BEANSTALKD_URLS'):
        connect_beanstalk(environ={'BEANSTALKD_URLS': '  '})
    with pytest.raises(ValueError, match='BEANSTALKD_URLS'):
        connect_beanstalk(environ={'BEANSTALKD_URLS': ' , ,'})


def test_connect_explicit_urls_skip_env():
    pool = connect_beanstalk('host1:11300', environ={}, initial_shuffle=False)
    assert [record.client.host for record in pool._clients] == ['host1']
    pool = connect_beanstalk(['host2:11300'], environ={}, initial_shuffle=False)
    assert [record.client.host for record in pool._clients] == ['host2']


def test_connect_defaults_forwarded():
    pool = connect_beanstalk('host1:11300', environ={}, initial_shuffle=False)
    record = pool._clients[0]
    assert record.client.socket_timeout == 3.0
    assert record.client.connect_timeout == 1.0
    assert record.client.auto_decode is False
    assert pool.backoff_time == 10.0
    assert pool.round_robin is True


def test_connect_overrides_forwarded():
    pool = connect_beanstalk(
        'host1:11300',
        environ={},
        initial_shuffle=False,
        socket_timeout=9.0,
        connect_timeout=5.0,
        backoff_time=42.0,
        round_robin=False,
        auto_decode=True,
    )
    record = pool._clients[0]
    assert record.client.socket_timeout == 9.0
    assert record.client.connect_timeout == 5.0
    assert record.client.auto_decode is True
    assert pool.backoff_time == 42.0
    assert pool.round_robin is False


def test_connect_initial_shuffle_false_preserves_order():
    pool = connect_beanstalk(
        'host1:11300,host2:11300,host3:11300', environ={}, initial_shuffle=False,
    )
    assert [record.client.host for record in pool._clients] == ['host1', 'host2', 'host3']


def test_production_pool_from_env():
    environ = {'BEANSTALKD_URLS': 'host1:11300,host2:11300'}
    pool = ProductionPool.from_env(environ=environ, initial_shuffle=False)
    assert [record.client.host for record in pool._clients] == ['host1', 'host2']
    assert pool._clients[0].client.socket_timeout == 3.0
    assert pool._clients[0].client.connect_timeout == 1.0
    assert pool.backoff_time == 10.0


def test_production_pool_from_env_missing_var_raises_value_error():
    with pytest.raises(ValueError, match='BEANSTALKD_URLS'):
        ProductionPool.from_env(environ={})


def test_connect_beanstalk_is_reexported():
    assert connect_beanstalk is helpers_connect_beanstalk


def test_connect_empty_explicit_urls_raises_value_error():
    with pytest.raises(ValueError, match='urls argument'):
        connect_beanstalk(' , ', environ={})
