import pystalk

import unittest


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


class SimpleBeanstalkTestCase(unittest.TestCase):
    def setUp(self):
        self.client = pystalk.BeanstalkClient('foo', 0)
        self.server = MockBeanstalkServerSocket()
        self.client.socket = self.server

    def test_stats(self):
        self.server.responses.append(b'OK 17\r\n--- {"foo": "bar"}\r\n')
        assert self.client.stats() == {'foo': 'bar'}
        assert self.server.received == [b'stats\r\n']
