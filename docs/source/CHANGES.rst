#################
pystalk ChangeLog
#################

=====
0.7.0
=====
* Drop support for Python < 3.6
* Add `pystalk.pool.ProductionPool`
* Add some `typing` annotations

=====
0.6.1
=====
* More emphatically remove `tests` from distributed packages

====
0.6.0
====
* add compatibility for `attrs` 19.2.0 and above
* drop support for Python 3.3.x

=====
0.5.1
=====
* attrs changed a kwarg from `convert` to `converter`

=====
0.5.0
=====
* Move tests from CircleCI to Travis-CI
* Add :py:func:`pystalk.client.BeanstalkClient.from_uri` to create a `BeanstalkClient` instance from a URI/URL

=====
0.4.0
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
