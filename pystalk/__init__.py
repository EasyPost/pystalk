from .client import BeanstalkClient, BeanstalkError

version_info = (0, 3, 0)
__version__ = '.'.join(str(s) for s in version_info)
__author__ = 'EasyPost <oss@easypost.com>'


__all__ = ['BeanstalkClient', 'BeanstalkError']
