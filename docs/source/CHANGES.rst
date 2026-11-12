#################
pystalk ChangeLog
#################

=====
0.9.3
=====
* Add :py:func:`pystalk.connect_beanstalk`, a helper that builds a
  :py:class:`pystalk.pool.ProductionPool` from a comma-separated list of
  beanstalkd URLs (read from the ``BEANSTALKD_URLS`` environment variable by
  default) with sensible defaults (3s socket timeout, 1s connect timeout, 10s
  backoff).
* Add :py:func:`pystalk.parse_beanstalk_urls`, which normalizes a
  comma-separated string or iterable of beanstalkd URLs (stripping whitespace
  and quotes, dropping empty entries, and prepending the ``beanstalk://``
  scheme when missing).
* Add :py:meth:`pystalk.pool.ProductionPool.from_env`, which constructs a pool
  from URLs read from an environment variable using the same logic as
  :py:func:`pystalk.connect_beanstalk`.

=====
0.9.2
=====
* **Behavior change:** ``OSError`` raised while sending or receiving a command
  (including ``socket.timeout``, ``TimeoutError``, ``ConnectionResetError``,
  and ``BrokenPipeError``) is now wrapped in
  :py:class:`pystalk.client.BeanstalkConnectionError`, chained from the
  original error (also preserved on :attr:`err`).
  :py:class:`~pystalk.client.BeanstalkConnectionError` is a
  :py:class:`~pystalk.client.BeanstalkError`, **not** an ``OSError``.
  ``except OSError`` and ``except socket.timeout`` around pystalk calls no
  longer catch these failures; catch
  :py:class:`~pystalk.client.BeanstalkConnectionError` or
  :py:class:`~pystalk.client.BeanstalkError` instead.
* Close the socket and reset client state on any connection-level error, and
  on any other exception that interrupts a command (for example
  ``KeyboardInterrupt``, a timeout raised by another library, or a response
  parse error), so the next call reconnects and restores ``USE`` / ``WATCH``
  instead of reading a stale reply. A normal protocol
  :py:class:`~pystalk.client.BeanstalkError` still leaves the socket open.
* :py:meth:`pystalk.pool.ProductionPool.put_job` and
  :py:meth:`~pystalk.pool.ProductionPool.put_job_into` pass the caller's
  ``ttr`` through. Previously both methods ignored it and stored ``ttr=120``.
* Treat a ``DRAINING`` response as retriable in
  :py:class:`~pystalk.pool.ProductionPool`, so a host drained for maintenance
  fails over to the next server.
* Add optional ``connect_timeout`` parameter to
  :py:class:`pystalk.client.BeanstalkClient`,
  :py:meth:`pystalk.client.BeanstalkClient.from_uri`,
  :py:meth:`pystalk.client.BeanstalkClient.ping`, and
  :py:meth:`pystalk.pool.ProductionPool.from_uris` (defaulting to
  ``socket_timeout`` when unset) so connection establishment can use a shorter
  timeout than socket I/O.

=====
0.9.1
=====
* Raise :py:class:`pystalk.client.BeanstalkConnectionError` when an established
  connection is closed by beanstalkd.

=====
0.9.0
=====
* Drop support for Python < 3.9
* Fix :py:meth:`pystalk.client.BeanstalkClient.put_job` to compute `put`
  payload length from UTF-8 encoded bytes, preventing `EXPECTED_CRLF` errors
  for non-ASCII job bodies.

=====
0.8.1
=====
* Fix :py:class:`pystalk.pool.ProductionPool` failover and backoff behavior so
  connection failures try each available server once before raising
  :py:class:`pystalk.pool.NoMoreClients`.

=====
0.8.0
=====
* Add :py:class:`pystalk.client.BeanstalkConnectionError`, a subclass of
  :py:class:`pystalk.client.BeanstalkError` raised when the underlying socket
  connection to beanstalkd fails (connection refused, timeout, or DNS errors),
  distinguishing connection-level failures from beanstalk protocol errors.
  Carries the `host` and `port` that were being connected to, and preserves
  the original socket error on :attr:`err`.
* Add :py:meth:`pystalk.client.BeanstalkClient.ping` to test connectivity
  without changing the client's active connection, current tube, or watchlist.
* Remove the legacy ``six`` compatibility dependency.

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

=====
0.6.0
=====
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
