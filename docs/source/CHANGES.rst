#################
pystalk ChangeLog
#################

=====
NEXT
=====
* Add :py:func:`pystalk.client.BeanstalkClient.peek_ready_iter` mirroring `peek_delayed_iter` and `peek_buried_iter`
* Add :py:func:`pystalk.client.BeanstalkClient.kick_job` to kick a single job by ID

======
0.3.1
======
* Fix crash when setting `socket_timeout` to `None`

======
0.3.0
======
* Add :py:func:`pystalk.client.BeanstalkClient.using` and :py:func:`pystalk.client.BeanstalkClient.put_job_into` as clearer APIs for inserting jobs
* Re-establish USE and WATCH on close/re-open

======
0.2.1
======

* Add :py:func:`pystalk.client.BeanstalkClient.close` method to the :py:class:`pystalk.client.BeanstalkClient` class
* Add a plethora of new and fixed docstrings
* Start hosting docs at http://pystalk.readthedocs.io

======
0.2
======

* Initial release
