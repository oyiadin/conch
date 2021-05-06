# coding=utf-8
import requests

from celery_workers.datafeeder import conf


def download(url: str, path: str) -> str:
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()
        with open(path, 'wb') as f:
            chunk_size = int(conf['network']['chunk_size'])
            for chunk in resp.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    return path


def explain_second(secs: float, format: str = "%.2f"):
    value = secs * 1000
    for unit_name, unit_scale in [
        ("ms", 1000),
        ("s", 60),
        ("min", 60),
        ("h", 24),
        ("d", 1000000000),
    ]:
        if 1 <= value < unit_scale:
            break
        value /= unit_scale
    return (format % value) + unit_name
