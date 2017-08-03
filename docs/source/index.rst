pystalk documentation
===================================

**pystalk** is a simple Python module for interacting with the
`beanstalk <https://kr.github.io/beanstalkd/>`_ task queueing daemon.
It doesn't provide much magic, but is suitable for building all sorts of
functionality on top of.

This work is available under the terms of the ISC License.

.. seealso::
    https://github.com/easypost/beancmd

Contents
--------

.. toctree::
   :maxdepth: 1

   CHANGES

Members
-------

.. autoclass:: pystalk.BeanstalkClient
   :members:
   :undoc-members:

.. autoclass:: pystalk.BeanstalkError
   :members:

.. autoclass:: pystalk.client.Job
   :members:

.. autoclass:: pystalk.client.BeanstalkInsertingProxy
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

