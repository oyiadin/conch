# coding=utf-8

import gzip
import os
import tempfile
from copy import deepcopy
from typing import Tuple, Dict

import requests
import lxml.etree as ET

from conch_streamin import conf, r, logger, t_dblp, app as celery_app


def download_file(url: str, path: str) -> str:
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()
        with open(path, 'wb') as f:
            chunk_size = int(conf['network']['chunk_size'])
            for chunk in resp.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    return path


def redownload_dtd_if_need(url: str = None):
    if url is None:
        url = conf['dblp']['dtd_url']
    with requests.head(conf['dblp']['dtd_url']) as resp:
        resp.raise_for_status()
        etag = resp.headers['ETag']
        last_etag = r.get('dblp_dtd_last_etag').decode()
        if last_etag and etag == last_etag:
            logger.debug("no need to re-download the dtd file")
        else:
            logger.info(f"dblp.dtd: etag {last_etag} -> {etag}, re-downloading")
            r.set('dblp_dtd_last_etag', etag)
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
                chunk = fr.read(int(conf['network']['chunk_size']))
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
    key = e.attrib['key']
    mdate = e.attrib['mdate']
    if e.tag in ['article', 'inproceedings']:
        authors = []
        for item in e.iterchildren(tag='author'):
            orcid = item.attrib.get('orcid', '')
            content = ' '.join(item.itertext())
            authors.append({'orcid': orcid, 'key': content})
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
            'key': key,
            'authors': authors,
            'booktitle': booktitle,
            'journal': journal,
            'volume': volume,
            'url': url,
            'ees': ees,
            'mdate': mdate,
            'year': year,
            'pages': pages,
            'notes': notes,
        }

    elif e.tag == 'www':
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
            'key': key,
            'urls': urls,
            'mdate': mdate,
            'notes': notes,
            'publtype': publtype,
        }

    else:
        raise ValueError(f"unknown tag type: {e.tag}")


def check_if_need_insert_or_update(info: Dict) -> bool:
    type = info['type']
    key = info['key']
    mdate = info['mdate']
    cached_mdate = r.get(f'dblp_{type}_{key}')
    if cached_mdate is not None:
        last_mdate = cached_mdate.decode()
    else:
        dblp_item = t_dblp.find_one({'key': key})
        if dblp_item is None:
            return True  # needed to be inserted
        last_mdate = dblp_item['mdate']
        r.set(f'dblp_{type}_{key}', mdate)  # TODO: 这个副作用放这不太好

    return mdate != last_mdate



def data_manage_of_article_or_inproceedings(info: Dict) -> Dict:
    copied_info = deepcopy(info)
    doi = ''
    for n, ee in enumerate(copied_info['ees']):
        if '://doi.org/' in ee:
            doi = ee
            del copied_info['ees'][n]
            break
    for n, note in enumerate(copied_info['notes']):
        if note['type'] == 'doi':
            doi = note['text']
            del copied_info['notes'][n]
            break
    copied_info['doi'] = doi
    copied_info['ees'].insert(0, copied_info['url'])
    del copied_info['url']
    del copied_info['mdate']
    copied_info['dblp_key'] = copied_info['key']
    del copied_info['key']

    return copied_info


def data_manage_of_homepages(info: Dict) -> Dict:
    copied_info = deepcopy(info)
    affiliations = []
    awards = []
    uname = ''
    for n, note in list(enumerate(copied_info['notes'])):
        if note['type'] == 'affiliation':
            affiliations.append({'label': note['label'], 'text': note['text']})
            # del copied_info['notes'][n]
        elif note['type'] == 'uname':
            uname = note['text']
            # del copied_info['notes'][n]
        elif note['type'] == 'award':
            awards.append({'label': note['label'], 'text': note['text']})
            # del copied_info['notes'][n]
    del copied_info['notes']
    copied_info['affiliations'] = affiliations
    copied_info['awards'] = awards
    copied_info['uname'] = uname
    del copied_info['mdate']
    del copied_info['type']
    copied_info['dblp_key'] = copied_info['key']
    del copied_info['key']

    return copied_info


def manage_an_update_or_insert(info: Dict):
    if info['type'] == 'article' or info['type'] == 'inproceedings':
        info = data_manage_of_article_or_inproceedings(info)
        celery_app.send_task("records.update_or_insert", kwargs=info)
    elif info['type'] == 'homepage':
        info = data_manage_of_homepages(info)
        celery_app.send_task("authors.update_or_insert", kwargs=info)
    else:
        raise ValueError(f"unknown item type: {info['type']}")
    # update redis mdate and db mdate
    t_dblp.update_one(
        {'key': info['key']},
        {'$set': {'mdate': info['mdate']}},
        upsert=True)
    r.set(f"dblp_{info['type']}_{info['key']}", info['mdate'])


def process_record(e: ET._Element):
    assert e.tag in ['article', 'inproceedings', 'www'], \
        "record tag must be one of article, inproceedings and www"
    publtype = e.attrib.get('publtype', '')
    if e.tag == 'article' or e.tag == 'inproceedings':
        if 'withdrawn' in publtype or publtype in ['data', 'software']:
            return  # do not handle it
    elif e.tag == 'www':
        key = e.attrib['key']
        if publtype == 'disambiguation' or publtype == 'noshow':
            return  # do not handle it
        if not key.startswith('homepages/'):
            return  # do not handle it

    info = extract_useful_information(e)
    if check_if_need_insert_or_update(info):
        logger.info(f"An item has updated (or newly inserted) by dblp: {e.attrib['key']}")
        manage_an_update_or_insert(info)


def analyze_xml(xml_path: str):
    class DTDResolver(ET.Resolver):
        def resolve(self, system_url, public_id, context):
            return self.resolve_filename("dblp.dtd", context)
    it = ET.iterparse(xml_path,
                      events=['start', 'end'],
                      tag=['dblp',
                           'phdthesis', 'book', 'mastersthesis',
                           'incollection', 'proceedings',
                           'article', 'inproceedings', 'www'],
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
            if elem.getparent() is not None:
                elem.getparent().clear()
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


if __name__ == '__main__':
    redownload_dtd_if_need()
    # gz_fd, gz_path = download_xml_gz('http://nginx/dblp.xml.gz')
    # xml_fd, xml_path = decompress_xml_gz(gz_path)
    # clean_tempfile(gz_fd, gz_path)
    # analyze_xml(xml_path)
    # clean_tempfile(xml_fd, xml_path)
    analyze_xml("dblp.xml")
