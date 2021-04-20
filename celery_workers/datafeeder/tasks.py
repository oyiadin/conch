# coding=utf-8
import copy
import gzip
import json
import os
import tempfile
import urllib.parse
from os import PathLike
from typing import Optional, Tuple, Dict, Literal

from celery_workers.datafeeder import *
from celery_workers.datafeeder.utils import *


def download_dblp_dtd(url: str = None) -> str:
    if url is None:
        url = conf['dblp']['dtd_url']
    fd, path = tempfile.mkstemp("datafeeder.dtd", "celery_workers")
    os.close(fd)
    to_path = download(url, path)
    logger.info("Downloaded dblp.dtd from %s to %s", url, to_path)
    return to_path


def download_dblp_xml_gz(url: str = None) -> str:
    if url is None:
        url = conf['dblp']['xml_gz_url']
    fd, path = tempfile.mkstemp("datafeeder.xml.gz", "celery_workers")
    os.close(fd)
    to_path = download(url, path)
    logger.info("Downloaded dblp.xml.gz from %s to %s", url, to_path)
    return to_path


@app.task(name="datafeeder.fetch_dblp")
def fetch_dblp(then_analyze: bool = False) -> Optional[Tuple[str, str]]:
    last_etag = r.get('dblp_last_xml_gz_etag')
    if last_etag is not None:
        last_etag = last_etag.decode()
    with requests.head(conf['dblp']['xml_gz_url']) as response:
        etag = response.headers['ETag']
        logger.debug("ETag of dblp.xml.gz: %s -> %s", last_etag, etag)
        if last_etag and etag == last_etag:
            logger.info("ETag of dblp.xml.gz hasn't changed, "
                        "stopping task fetch_dblp")
        else:
            r.set('dblp_last_xml_gz_etag', etag)
            logger.info("Inspected ETag change of dblp.xml.gz")
            dtd_path = download_dblp_dtd()
            xml_gz_path = download_dblp_xml_gz()

            if then_analyze:
                analyze_dblp.delay(dtd_path, xml_gz_path)
            return dtd_path, xml_gz_path


def decompress_xml_gz(xml_gz_path: PathLike, to_path: PathLike = None) -> str:
    if to_path is None:
        fd, to_path = tempfile.mkstemp("dblp.xml", "celery_workers")
        os.close(fd)
    with gzip.open(xml_gz_path, 'rb') as fr:
        with open(to_path, 'wb') as fw:
            chunk_size = int(conf['network']['chunk_size'])
            logger.debug(
                "Decompressing dblp.xml.gz with chunk_size=%d: %s -> %s",
                chunk_size, xml_gz_path, to_path)
            while True:
                chunk = fr.read(chunk_size)
                if chunk:
                    fw.write(chunk)
                else: break
    logger.info("Extracted dblp.XML out of dblp.xml.GZ to %s", to_path)
    return to_path


def extract_info(e: ET._Element) -> Dict:
    assert e.tag in ['article', 'inproceedings', 'www']
    item: ET._Element  # to make IDE know what "item" is

    # common attributes
    key = e.attrib['key']
    mdate = e.attrib['mdate']

    if e.tag in ['article', 'inproceedings']:
        authors = []
        for item in e.iterchildren(tag='author'):
            orcid = item.attrib.get('orcid', '')
            content = ' '.join(item.itertext())
            authors.append({'orcid': orcid, 'key': content})

        # one title only
        title = get_inside_html(next(e.iterchildren('title')))
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

        journal = volume = booktitle = ''
        # extract the article-specific attributes
        if e.tag == 'article':
            journal = ''
            for item in e.iterchildren('journal'):
                journal = item.text
                break
            volume = ''
            for item in e.iterchildren('volume'):
                volume = item.text
                break
        else:  # inproceedings
            booktitle = ' '.join(next(e.iterchildren('booktitle')).itertext())

        ret = {
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
        names = []
        for item in e.iterchildren(tag='author'):
            names.append(' '.join(item.itertext()))
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

        ret = {
            'type': 'homepage',
            'names': names,
            'key': key,
            'urls': urls,
            'mdate': mdate,
            'notes': notes,
            'publtype': publtype,
        }

    else:
        raise ValueError(f"unknown tag type: {e.tag}")

    logger.debug("Extracted information: %s", json.dumps(ret))
    return ret


def is_valuable(e: ET._Element, info: Dict) -> bool:
    assert e.tag in ['article', 'inproceedings', 'www']

    publtype = e.attrib.get('publtype', '')

    if e.tag == 'article' or e.tag == 'inproceedings':
        if 'withdrawn' in publtype or publtype in ['data', 'software']:
            return False
        if not info['url']:
            return False

    elif e.tag == 'www':
        key = e.attrib['key']
        if publtype == 'disambiguation' or publtype == 'noshow':
            return False
        if not key.startswith('homepages/'):
            return False

    return True


@app.task(name="datafeeder.analyze_dblp")
def analyze_dblp(dtd_path: PathLike, xml_gz_path: PathLike):
    last_started = r.get('dblp_last_analyze_started')
    if last_started:
        logger.error("The last analyze_dblp task has not finished yet")
        return
    r.set('dblp_last_analyze_started', 1)

    xml_path = decompress_xml_gz(xml_gz_path)

    class DTDResolver(ET.Resolver):
        def resolve(self, system_url, public_id, context):
            return self.resolve_filename(dtd_path, context)

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
                info = extract_info(elem)
                if is_valuable(elem, info):
                    if info['type'] in ['article', 'inproceedings']:
                        process_record.delay(info)
                    else:  # homepage
                        process_homepage.delay(info)
                else:
                    logger.debug("One element was ignored with key=%s",
                                 info['key'])
            parent = elem.getparent()
            if parent is not None:
                parent.clear()
            elem.clear()
        else:
            raise ValueError(f"Unknown SAX event: {event}")

    r.delete('dblp_last_analyze_started')
    os.remove(xml_gz_path)
    os.remove(xml_path)
    logger.debug("Removed useless files: %s; %s", xml_gz_path, xml_path)


def match_action(type: Literal["article", "inproceedings", "homepage"],
                 key: str, mdate: str) -> Optional[Literal["insert", "update"]]:
    logged_key = f'dblp_{type}_{key}'
    cached_mdate = r.get(logged_key)
    if cached_mdate is not None:
        last_mdate = cached_mdate.decode()
        logger.debug(
            "Sucessfully fetched the cached mdate of %s from redis: %s",
            logged_key, cached_mdate)
    else:
        logger.debug("Cannot fetch the mdate of %s from redis", logged_key)
        dblp_item = t_dblp.find_one({'key': logged_key})
        if dblp_item is None:
            logger.debug("The action of %s is INSERT", logged_key)
            return "insert"
        last_mdate = dblp_item['mdate']

    if mdate != last_mdate:
        logger.debug("The action of %s is UPDATE", logged_key)
        return "update"


def translate_record(info: Dict) -> Dict:
    doc = copy.deepcopy(info)

    doi = ''
    for n, ee in enumerate(doc['ees']):
        if '://doi.org/' in ee:
            doi = ee
            del doc['ees'][n]
            break
    for n, note in enumerate(doc['notes']):
        if note['type'] == 'doi':
            doi = note['text']
            del doc['notes'][n]
            break
    doc['doi'] = doi

    doc['ees'].insert(0, urllib.parse.urljoin(conf['dblp']['dblp_url'],
                                              doc['url']))
    del doc['url']

    # 1. rename author.key to author.datafeeder_key
    # 2. add a field 'name' for number-suffix-stripped name
    for author in doc['authors']:
        author['datafeeder_key'] = author['key']
        purified_name = ' '.join(filter(lambda x: not x.isdigit(),
                                        author['key'].split(' ')))
        author['name'] = purified_name
        del author['key']

    del doc['mdate']

    doc['dblp_key'] = doc['key']
    del doc['key']

    doc['abstract'] = ''

    return doc


@app.task(name="datafeeder.process_record")
def process_record(info: Dict):
    assert info['type'] in ['article', 'inproceedings']

    type, key, mdate = info['type'], info['key'], info['mdate']
    logged_key = f'dblp_{type}_{key}'
    action = match_action(type, key, mdate)

    if action:
        r.set(logged_key, mdate)

        doc = translate_record(info)
        logger.debug("Translated record: %s", json.dumps(doc))

        task_name = "records.insert" if action == 'insert' else "records.update"
        app.send_task(task_name, args=(doc,))

        t_dblp.update_one(
            {'key': logged_key}, {'$set': {'mdate': mdate}}, upsert=True)
        r.set(logged_key, mdate)

        logger.info("A record processed: %s", logged_key)

    else:
        logger.debug("No need to process the record %s", logged_key)


def translate_homepage(info: Dict) -> Dict:
    doc = copy.deepcopy(info)

    uname = ''
    awards, affiliations = [], []
    for n, note in list(enumerate(doc['notes'])):
        if note['type'] == 'uname':
            uname = note['text']
            # del doc['notes'][n]
        elif note['type'] == 'award':
            awards.append({'label': note['label'], 'text': note['text']})
            # del doc['notes'][n]
        elif note['type'] == 'affiliation':
            affiliations.append({'label': note['label'], 'text': note['text']})
            # del doc['notes'][n]
    doc['uname'] = uname
    doc['awards'] = awards
    doc['affiliations'] = affiliations
    del doc['notes']

    doc['is_disambiguation'] = doc['publtype'] == 'disambiguation'
    del doc['publtype']

    doc['dblp_homepage'] = urllib.parse.urljoin(
        conf['dblp']['dblp_url'], doc['key'].replace('homepages', 'pid'))
    del doc['key']

    del doc['type']
    del doc['mdate']

    doc['datafeeder_keys'] = doc['names']

    purified_names = [' '.join(
        filter(lambda x: not x.isdigit(), name.split(' ')))
        for name in doc['names']]
    doc['names'] = purified_names

    # make the (maybe) fullest name as the first one
    if len(purified_names) > 1:
        for n, name in enumerate(purified_names):
            if '.' not in name and len(name) > len(purified_names[0]):
                if n == 0:
                    break
                doc['names'] = copy.copy(purified_names)
                doc['names'].remove(name)
                doc['names'].insert(0, name)
                break

    return doc


@app.task(name="datafeeder.process_homepage")
def process_homepage(info: Dict):
    assert info['type'] == 'homepage'

    type, key, mdate = info['type'], info['key'], info['mdate']
    logged_key = f'dblp_{type}_{key}'
    action = match_action(type, key, mdate)

    if action:
        r.set(logged_key, mdate)

        doc = translate_homepage(info)
        logger.debug("Translated homepage: %s", json.dumps(doc))

        task_name = "authors.insert" if action == 'insert' else "authors.update"
        app.send_task(task_name, args=(doc,))

        t_dblp.update_one(
            {'key': logged_key}, {'$set': {'mdate': mdate}}, upsert=True)
        r.set(logged_key, mdate)

        logger.info("A homepage processed: %s", logged_key)

    else:
        logger.debug("No need to process the homepage %s", logged_key)
