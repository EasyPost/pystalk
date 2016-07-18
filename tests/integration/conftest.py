import tempfile
import shutil
import socket
import subprocess
import os.path
import random
import time

import pytest

import pystalk


@pytest.yield_fixture(scope='session')
def tmpdir_session():
    d = tempfile.mkdtemp()
    try:
        yield d
    finally:
        shutil.rmtree(d)


@pytest.yield_fixture(scope='session')
def beanstalkd(tmpdir_session):
    wal_dir = os.path.join(tmpdir_session, 'wal')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('127.0.0.1', 0))
    _, port = s.getsockname()
    s.close()
    os.makedirs(wal_dir)
    p = subprocess.Popen([
        'beanstalkd',
        '-b', wal_dir,
        '-l', '127.0.0.1',
        '-p', str(port)
    ])
    # give it a moment to start up
    time.sleep(0.25)
    if p.poll() is not None:
        raise ValueError('Could not start beanstalkd')
    try:
        yield ('127.0.0.1', port)
    finally:
        p.terminate()
        for _ in range(10):
            time.sleep(0.1)
            if p.poll() is not None:
                return
        p.kill()
        p.wait()


@pytest.fixture
def beanstalk_client(beanstalkd):
    host, port = beanstalkd
    client = pystalk.BeanstalkClient(host, port, socket_timeout=10)
    return client


@pytest.fixture
def tube_name():
    return ''.join(chr(random.choice(range(97, 123))) for _ in range(8))
