# coding=utf-8
import gzip
import os
import tempfile
from copy import deepcopy
from typing import Tuple, Literal, Optional, Dict
from zlib import crc32

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


def extract_useful_information(e: ET._Element) -> Dict:
    item: ET._Element
    if e.tag in ['article', 'inproceedings']:
        authors = []
        for item in e.iterchildren(tag='author'):
            orcid = item.attrib.get('orcid', '')
            content = ' '.join(item.itertext())
            authors.append({'orcid': orcid, 'name': content})
        title = get_html_inside(next(e.iterchildren('title')))
        pages = ''
        for item in e.iterchildren('pages'):
            pages = item.text
            break
        year = ''
        for item in e.iterchildren('year'):
            year = item.text
            break
        ees = []
        for item in e.iterchildren('ee'):
            content = item.text
            ees.append(content)

        if e.tag == 'article':
            journal = ''
            for item in e.iterchildren('journal'):
                journal = item.text
                break
            volume = ''
            for item in e.iterchildren('volume'):
                volume = item.text
                break
            url = ''
            for item in e.iterchildren('url'):
                url = item.text
                break
            notes = []
            for item in e.iterchildren('note'):
                type = item.attrib.get('type', '')
                if type == 'reviewid' or type == 'rating':
                    continue
                else:
                    notes.append({'type': type, 'text': item.text})

            booktitle = ''

        # elif e.tag == 'inproceedings':
        else:
            booktitle = ' '.join(next(e.iterchildren('booktitle')).itertext())
            url = next(e.iterchildren('url')).text
            notes = []
            for item in e.iterchildren('note'):
                notes.append({'type': '', 'text': item.text})

            journal = volume = ''

        return {
            'type': e.tag,
            'title': title,
            'authors': authors,
            'booktitle': booktitle,
            'journal': journal,
            'volume': volume,
            'url': url,
            'ees': ees,
            'year': year,
            'pages': pages,
            'notes': notes,
        }

    elif e.tag == 'www':
        mdate = e.attrib['mdate']
        publtype = e.attrib.get('publtype', '')
        name = ' '.join(next(e.iterchildren('author')).itertext())
        notes = []
        for item in e.iterchildren(tag='note'):
            type = item.attrib.get('type', '')
            label = item.attrib.get('label', '')
            content = ' '.join(item.itertext())
            notes.append({'type': type, 'label': label, 'text': content})
        urls = []
        for item in e.iterchildren(tag='url'):
            type = item.attrib.get('type', '')
            if type == 'deprecated':
                continue
            content = ' '.join(item.itertext())
            urls.append({'type': type, 'text': content})
        # <ee> is useless in most of the homepages

        return {
            'type': 'homepage',
            'name': name,
            'urls': urls,
            'mdate': mdate,
            'notes': notes,
            'publtype': publtype,
        }

    else:
        raise ValueError(f"unknown tag type: {e.tag}")


def calc_article_or_in_proceedings_hash(info) -> int:
    copied_info = deepcopy(info)
    copied_info['authors'] = ';'.join(
        f"{item['orcid']}|{item['name']}" for item in info['authors'])
    copied_info['ees'] = ';'.join(info['ees'])
    copied_info['notes'] = ';'.join(
        f"{item['type']}:{item['text']}" for item in info['notes'])
    description = (
        '{type};{authors}{title}{booktitle}{journal}{volume}{year}{pages}'
        ';U{url};N{notes};E{ees}'
    ).format(**copied_info)
    return crc32(description)


def calc_www_homepages_hash(info: Dict) -> int:
    copied_info = deepcopy(info)
    copied_info['urls'] = ';'.join(
        f"{item['type']}|{item['text']}" for item in info['urls'])
    copied_info['notes'] = ';'.join(
        f"{item['type']}/{item['label']}={item['text']}"
        for item in info['notes'])
    description = \
        '{type};{name}{mdate}{publtype};N{notes};U{urls}'.format(**copied_info)
    return crc32(description)


def check_if_need_insert_or_update(e: ET._Element) -> bool:
    hash = dict(
        article=calc_article_or_in_proceedings_hash,
        inproceedings=calc_article_or_in_proceedings_hash,
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



