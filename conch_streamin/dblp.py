# coding=utf-8
import gzip
import os
import tempfile
from typing import Tuple, Literal, Optional

import requests
import lxml.etree as ET

from conch_streamin.main import conf, r, logger, t_dblp


def download_file(url: str, path: str) -> str:
    with requests.get(url, stresm=True) as resp:
        resp.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=conf['network']['chunk_size']):
                f.write(chunk)

    return path


def redownload_dtd_if_need(url: str = None):
    if url is None:
        url = conf['dblp']['dtd_url']
    with requests.head(conf['dblp']['dtd_url']) as resp:
        resp.raise_for_status()
        etag = resp.headers['ETag']
        last_etag = r.get('dblp_dtd_last_etag')
        if last_etag and etag == last_etag:
            logger.debug("no need to re-download the dtd file")
        else:
            logger.info(f"dblp.dtd: etag {last_etag} -> {etag}, re-downloading")
            download_file(url, conf['dblp']['dtd_localpath'])


def download_xml_gz(url: str = None) -> Tuple[int, str]:
    if url is None:
        url = conf['dblp']['url']
    fd, path = tempfile.mkstemp("streamin.xml.gz", "conch")
    download_file(url, path)
    return fd, path


def decompress_xml_gz(gz_path: str) -> Tuple[int, str]:
    fd, path = tempfile.mkstemp("dblp.xml", "conch")
    with gzip.open(gz_path, 'rb') as fr:
        with open(path, 'wb') as fw:
            while True:
                chunk = fr.read(conf['network']['chunk_size'])
                if chunk:
                    fw.write(chunk)
                else: break

    return fd, path


def get_html_inside(root: ET._Element) -> str:
    result = ''
    for event, elem in ET.iterwalk(root, ['start', 'end']):
        if event == 'start':
            if elem is not root and not isinstance(elem, ET._Entity):
                    result += f'<{elem.tag}>{elem.text}'
            else:
                result += elem.text
        else:
            if elem is not root:
                if not isinstance(elem, ET._Entity):
                    result += f'</{elem.tag}>{elem.tail}'
                else:
                    result += elem.tail
            else: pass  # omit the tail of <root>

    return result



def calc_article_hash(e: ET._Element) -> int:
    pass


def calc_inproceedings_hash(e: ET._Element) -> int:
    pass


def calc_www_homepages_hash(e: ET._Element) -> int:
    mdate = e.attrib['mdate']
    publtype = e.attrib.get('publtype', '')
    author = ' '.join(next(e.iterchildren('author')).itertext())

    description = ''


def check_if_need_insert_or_update(e: ET._Element) -> bool:
    hash = dict(
        article=calc_article_hash,
        inproceedings=calc_inproceedings_hash,
        www=calc_www_homepages_hash,
    )[e.tag](e)
    key = e.attrib['key']  # key is required in dblp.xml
    cached_hash = r.get(f'dblp_{key}')
    if cached_hash is not None:
        last_hash = int(cached_hash)
    else:
        dblp_item = t_dblp.find_one({'key': key})
        if dblp_item is None:
            return True  # needed to be inserted
        last_hash = dblp_item['hash']

    return hash != last_hash


def update_or_insert_to_db(e: ET._Element):
    pass


def process_record(e: ET._Element):
    assert e.tag in ['article', 'inproceedings', 'www'], \
        "record tag must be one of article, inproceedings and www"
    if e.tag == 'www':
        key = e.attrib['key']
        publtype = e.attrib.get('publtype')
        if publtype == 'disambiguation' or publtype == 'noshow':
            return  # do not handle it
        if not key.startswith('homepages/'):
            return  # do not handle it

    if check_if_need_insert_or_update(e):
        logger.info(f"An item has updated by dblp: {e.attrib['key']}")
        update_or_insert_to_db(e)


def analyze_xml(xml_path: str):
    class DTDResolver(ET.Resolver):
        def resolve(self, system_url, public_id, context):
            return self.resolve_filename("dblp.dtd", context)
    it = ET.iterparse(xml_path,
                      events=['start', 'end'],
                      tag=['dblp', 'article', 'inproceedings', 'www'],
                      load_dtd=True)
    it.resolvers.add(DTDResolver())

    elem: ET._Element
    for event, elem in it:
        if event == 'start':
            if elem.tag == 'dblp':
                elem.clear()  # remove the root node to preserve memory
        elif event == 'end':
            if elem.tag in ['article', 'inproceedings', 'www']:
                process_record(elem)
                elem.clear()
        else:
            logger.error(f"unknown xml sax event: {event}")

def clean_tempfile(fd, path):
    os.close(fd)
    os.remove(path)


def dblp_analyze_entrance():
    redownload_dtd_if_need()
    gz_fd, gz_path = download_xml_gz()
    xml_fd, xml_path = decompress_xml_gz(gz_path)
    clean_tempfile(gz_fd, gz_path)
    analyze_xml(xml_path)
    clean_tempfile(xml_fd, xml_path)



