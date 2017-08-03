import pytest

from pystalk import BeanstalkError


def test_simple_flow(beanstalk_client, tube_name):
    # make sure the tube does not already exist
    with pytest.raises(BeanstalkError):
        beanstalk_client.stats_tube(tube_name)
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('some data')
    stats = beanstalk_client.stats_tube(tube_name)
    assert stats['current-jobs-ready'] == 1


def test_consume_jobs(beanstalk_client, tube_name):
    # first, let's put a bunch of jobs into the tube
    beanstalk_client.use(tube_name)
    for i in range(100):
        beanstalk_client.put_job(str(i))
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 100
    i_s = []
    assert beanstalk_client.watchlist == set(['default'])
    beanstalk_client.watch(tube_name)
    assert beanstalk_client.watchlist == set([tube_name])
    for job in beanstalk_client.reserve_iter():
        i_s.append(int(job.job_data))
        beanstalk_client.delete_job(job)
    assert i_s == list(range(100))
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 0


def test_pause_tube_unpause_tube(beanstalk_client, tube_name):
    # put some jobs into the queue
    beanstalk_client.use(tube_name)
    for i in range(2):
        beanstalk_client.put_job(str(i))
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 2
    beanstalk_client.watch(tube_name)
    # pause the tube
    beanstalk_client.pause_tube(tube_name, delay=30)
    assert beanstalk_client.stats_tube(tube_name)['cmd-pause-tube'] == 1
    # check that we can' reserve a job
    with pytest.raises(BeanstalkError) as e:
        beanstalk_client.reserve_job(0)
    assert e.value.message == 'TIMED_OUT'
    # unpause the tube and wait a moment for it to take effect
    beanstalk_client.unpause_tube(tube_name)
    job = beanstalk_client.reserve_job(1)
    assert job


def test_list_tubes(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('foobar')
    assert tube_name in beanstalk_client.list_tubes()


def test_watch_ignore(beanstalk_client, tube_name):
    assert beanstalk_client.watchlist == set(['default'])
    beanstalk_client.watch(tube_name)
    assert beanstalk_client.watchlist == set([tube_name])
    beanstalk_client.watch('default')
    assert beanstalk_client.watchlist == set([tube_name, 'default'])
    beanstalk_client.ignore(tube_name)
    assert beanstalk_client.watchlist == set(['default'])
    with pytest.raises(KeyError):
        beanstalk_client.ignore('0')


def test_stats_job(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('banana')
    beanstalk_client.watch(tube_name)
    job = beanstalk_client.reserve_job(0)
    stats_job = beanstalk_client.stats_job(job.job_id)
    assert stats_job['state'] == 'reserved'
    assert stats_job['tube'] == tube_name


def test_peek_ready(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('test_peek_ready')
    job = beanstalk_client.peek_ready()
    assert job.job_data == b'test_peek_ready'


def test_peek_delayed(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('test_peek_delayed', delay=30)
    with pytest.raises(BeanstalkError):
        beanstalk_client.reserve_job(0)
    job = beanstalk_client.peek_delayed()
    assert job.job_data == b'test_peek_delayed'


def test_peek_delayed_empty(beanstalk_client, tube_name):
    with pytest.raises(BeanstalkError) as e:
        beanstalk_client.peek_delayed()
    assert e.value.message == 'NOT_FOUND'


def test_peek_buried(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('test_peek_buried')
    # reserve it
    beanstalk_client.watch(tube_name)
    job = beanstalk_client.reserve_job(0)
    assert job.job_data == b'test_peek_buried'
    beanstalk_client.bury_job(job)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-buried'] == 1
    job = beanstalk_client.peek_buried()
    assert job.job_data == b'test_peek_buried'


def test_release_job(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('test_release_job')
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 1
    beanstalk_client.watch(tube_name)
    job = beanstalk_client.reserve_job(0)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 0
    beanstalk_client.release_job(job)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 1


def test_kick_jobs(beanstalk_client, tube_name):
    beanstalk_client.use(tube_name)
    for i in range(10):
        beanstalk_client.put_job('test_kick_jobs_' + str(i))
    beanstalk_client.watch(tube_name)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 10
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-buried'] == 0
    for i in range(10):
        job = beanstalk_client.reserve_job(0)
        beanstalk_client.bury_job(job)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 0
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-buried'] == 10
    beanstalk_client.kick_jobs(5)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 5
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-buried'] == 5


def test_auto_decode(beanstalk_client, tube_name):
    beanstalk_client.auto_decode = True
    beanstalk_client.use(tube_name)
    beanstalk_client.put_job('test_auto_decode')
    assert beanstalk_client.peek_ready().job_data == 'test_auto_decode'


def test_restore_after_disconnect(beanstalk_client, tube_name):
    beanstalk_client.watch(tube_name)
    beanstalk_client.use(tube_name)
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 0
    beanstalk_client.close()
    # make sure the job still gets put into the right tube
    beanstalk_client.put_job('test_job')
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 1
    j = beanstalk_client.reserve_job(0)
    assert j.job_data == b'test_job'


def test_using(beanstalk_client, tube_name):
    beanstalk_client.use('default')
    with beanstalk_client.using(tube_name) as inserter:
        inserter.put_job(b'test_job')
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 1


def test_put_job_into(beanstalk_client, tube_name):
    beanstalk_client.put_job_into(tube_name, 'some_job')
    assert beanstalk_client.stats_tube(tube_name)['current-jobs-ready'] == 1
