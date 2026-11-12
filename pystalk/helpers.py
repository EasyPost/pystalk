import os
from typing import Iterable, List, Mapping, Optional, Union

from .pool import ProductionPool


def parse_beanstalk_urls(urls: Union[str, Iterable[str]]) -> List[str]:
    """Parse beanstalkd URLs into a list of URIs suitable for :meth:`ProductionPool.from_uris`.

    Accepts either a comma-separated string (e.g. the contents of a ``BEANSTALKD_URLS``
    environment variable) or an iterable of strings. Each entry is stripped of surrounding
    whitespace and single/double quotes, empty entries are dropped, and entries without a
    scheme have ``beanstalk://`` prepended (IPv6 literals such as ``[::1]:11300`` are
    supported). Scheme validation is left to :meth:`BeanstalkClient.from_uri`.

    :param urls: A comma-separated string of URLs, or an iterable of URL strings
    :returns: A list of ``beanstalk://``/``beanstalkd://`` URIs
    """
    entries: Iterable[str]
    if isinstance(urls, str):
        entries = urls.split(',')
    else:
        entries = urls
    uris = []
    for raw_entry in entries:
        entry = raw_entry.strip(" \t\r\n\"'")
        if not entry:
            continue
        if '://' not in entry:
            entry = f'beanstalk://{entry}'
        uris.append(entry)
    return uris


def connect_beanstalk(urls: Optional[Union[str, Iterable[str]]] = None, *,
                      env_var: str = 'BEANSTALKD_URLS',
                      socket_timeout: Optional[float] = 3.0,
                      connect_timeout: Optional[float] = 1.0,
                      backoff_time: float = 10.0,
                      round_robin: bool = True,
                      initial_shuffle: bool = True,
                      auto_decode: bool = False,
                      environ: Optional[Mapping[str, str]] = None) -> ProductionPool:
    """Build a :class:`ProductionPool` from a list of beanstalkd URLs or an environment variable.

    If ``urls`` is None, the URLs are read from ``environ[env_var]`` (``environ`` defaults to
    ``os.environ``) as a comma-separated string. URLs are parsed with
    :func:`parse_beanstalk_urls`. Raises :class:`ValueError` if the environment variable is
    missing or if no URLs remain after parsing. No network connections are made; constructing
    clients does not connect.

    :param urls: A comma-separated string of URLs, an iterable of URL strings, or None to
        read from the environment
    :param env_var: Name of the environment variable to read when ``urls`` is None
    :param socket_timeout: Socket timeout to set on all constructed clients
    :param connect_timeout: Connect timeout to set on all constructed clients
    :param backoff_time: Number of seconds after an error before a server will be reused
    :param round_robin: If true, every insertion will go to a different server in the pool
    :param initial_shuffle: Randomly shuffle clients at initialization
    :param auto_decode: Whether bodies should be bytes (False) or strings (True)
    :param environ: Mapping to read environment variables from (defaults to ``os.environ``)
    :returns: A :class:`ProductionPool` for the given URLs
    """
    if environ is None:
        environ = os.environ
    source = 'the urls argument'
    if urls is None:
        source = f'environment variable {env_var}'
        try:
            urls = environ[env_var]
        except KeyError:
            raise ValueError(f'No URLs given and environment variable {env_var} is not set') from None
    uris = parse_beanstalk_urls(urls)
    if not uris:
        raise ValueError(f'No beanstalkd URLs found ({source} is empty or has no valid entries)')
    return ProductionPool.from_uris(
        uris,
        socket_timeout=socket_timeout,
        connect_timeout=connect_timeout,
        auto_decode=auto_decode,
        round_robin=round_robin,
        backoff_time=backoff_time,
        initial_shuffle=initial_shuffle,
    )
