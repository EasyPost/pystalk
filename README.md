**`pystalk`** is an extremely simple client for [beanstalkd](http://kr.github.io/beanstalkd/). It is
compatible with both Python 2 and Python 3.

This project was initially created for [beancmd](https://github.com/EasyPost/beancmd). You may also be interested in that tool!

[![CircleCI](https://circleci.com/gh/EasyPost/pystalk.svg?style=svg&circle-token=6541f20728b477392b8e30e80b5f4cc573ba8833)](https://circleci.com/gh/EasyPost/pystalk)
[![ReadTheDocs](https://readthedocs.org/projects/pip/badge/?version=latest)](http://pystalk.readthedocs.io/en/latest/)

## Requirements / Installing

This software works with Python 2.7, 3.3, 3.4, and 3.5. It'll probably work with Python 3.6 as well.

It does not support any asynchronous event loops and has not been tested with gevent. It's designed for simple,
synchronous use.

You should be able to install it from [PyPI](https://pypi.python.org) with `pip install pystalk`.

## Example Usage

### Creating Jobs

```lang=python
#!/usr/bin/python

import json

from pystalk import BeanstalkClient

client = BeanstalkClient('10.0.0.1', 11300)
client.put_job(json.dumps({"foo": "bar"}), delay=30)
```

This will create a job with a 30-second delay on it. Note that the data for a job must be UTF-8 encodable.

#### Creating Jobs in Specific Tubes

Beanstalk has a notion of `tube`s (which is to say, named queues). There are several ways to put a
job into a specific tube using pystalk:

```lang=python
#!/usr/bin/python

from pystalk import BeanstalkClient

client = BeanstalkClient('10.0.0.1', 11300)

# method 1, matches the upstream protocol
client.use("some_tube")
client.put_job("some message")

# method 2, using an external guard object like you would in C++ or Rust
with client.using("some_tube") as inserter:
    inserter.put_job("some message")


# method 3
client.put_job_into("some_tube", "some message")
```

### Consuming All Available Jobs

The following script will walk through all currently-READY jobs and then exit:

```lang=python
#!/usr/bin/python

from pystalk import BeanstalkClient

client = BeanstalkClient('10.0.0.1', 11300)
for job in client.reserve_iter():
    try:
        execute_job(job)
    except Exception:
        client.release_job(job.job_id)
        raise
    client.delete_job(job.job_id)
```

Note that, even though we require that job data be UTF-8 encodeable in the `put_job` method, we do not decode for you -- the job data that comes out is a byte-string in Python 3.5. You should call `.decode("utf-8")` on it if you want to get the input data back out. If you would like that behavior, pass `auto_decode=True` to the `BeanstalkClient` constructor; note that this might make it difficult for you to consume data injected by other systems which don't assume UTF-8.

### Multiple Job Servers

The following will reserve jobs from a group of Beanstalk servers, fairly rotating between them.

```lang=python
#!/usr/bin/python

from myapp import execute_job
from pystalk import BeanstalkClient, BeanstalkTimedOutError

hosts = ('10.0.0.1', '10.0.0.2')
clients = dict((h, BeanstalkClient(h, 11300)) for h in hosts)

i = 0
while True:
    i += 1
    client = clients[hosts[i % len(hosts)]]
    try:
        job = client.reserve_job(1)
    except BeanstalkError as e:
        if e.message == 'TIMED_OUT':
            continue
        else:
            raise
    execute_job(job)
    client.delete_job(job.job_id)
```

## Development

Pretty straightforward. Develop in branches, send PRs, land on master. All tests must pass before landing.

### Releasing a new version

   1. Land all requisite changes
   1. Bump the version in `setup.py` and `pystalk/__init__.py` to the stable version (e.g., `0.2`)
   1. Update [`CHANGES.rst`](docs/source/CHANGES.rst) with the changes and the new version number
   1. Update [`conf.py`](docs/source/conf.py) with the new version number
   1. Commit
   1. Tag the version (e.g., `git tag -s pystalk-0.2`)
   1. Push up to Github
   1. Upload to PyPI with `python setup.py sdist upload`
