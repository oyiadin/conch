# coding=utf-8
import lxml.etree as ET
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


def get_inside_html(root: ET._Element) -> str:
    html = ''
    for event, elem in ET.iterwalk(root, ['start', 'end']):
        if event == 'start':
            if elem is not root and not isinstance(elem, ET._Entity):
                    html += f'<{elem.tag}>{elem.text}'
            else:
                html += elem.text
        else:
            if elem is not root:
                if not isinstance(elem, ET._Entity):
                    html += f'</{elem.tag}>{elem.tail}'
                else:
                    html += elem.tail
            else: pass  # omit the tail of <root>

    return html
