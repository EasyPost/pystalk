**`pystalk`** is an extremely simple client for [beanstalkd](http://kr.github.io/beanstalkd/). It is
compatible with both Python 2 and Python 3.

This project was initially created for [beancmd](https://github.com/EasyPost/beancmd). You may also be interested in that tool!

[![CircleCI](https://circleci.com/gh/EasyPost/pystalk.svg?style=svg&circle-token=6541f20728b477392b8e30e80b5f4cc573ba8833)](https://circleci.com/gh/EasyPost/pystalk)

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

Note that, even though we require that job data be UTF-8 encodeable in the `put_job` method, we do not decode for you --
the job data that comes out is a byte-string in Python 3.5. You should call `.decode("utf-8")` on it if you want to get
the input data back out.

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
