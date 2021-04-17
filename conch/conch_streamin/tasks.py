# coding=utf-8
import copy
import gzip
import os
import tempfile
import urllib.parse
from os import PathLike
from typing import Optional, Tuple, Dict, Literal

from conch.conch_streamin import *
from conch.conch_streamin.utils import *


def download_dblp_dtd(url: str = None) -> str:
    if url is None:
        url = conf['dblp']['dtd_url']
    fd, path = tempfile.mkstemp("streamin.dtd", "conch")
    os.close(fd)
    return download(url, path)


def download_dblp_xml_gz(url: str = None) -> str:
    if url is None:
        url = conf['dblp']['xml_gz_url']
    fd, path = tempfile.mkstemp("streamin.xml.gz", "conch")
    os.close(fd)
    return download(url, path)


@app.task(name="streamin.fetch_dblp")
def fetch_dblp(then_analyze: bool = False) -> Optional[Tuple[str, str]]:
    last_etag = r.get('dblp_last_xml_gz_etag')
    with requests.head(conf['dblp']['xml_gz_url']) as response:
        etag = response.headers['ETag']
        if last_etag and etag == last_etag:
            logger.info("dblp etag not changed, stopping fetching")
        else:
            r.set('dblp_last_xml_gz_etag', etag)
            dtd_path = download_dblp_dtd()
            xml_gz_path = download_dblp_xml_gz()

            if then_analyze:
                analyze_dblp.delay(dtd_path, xml_gz_path)
            return dtd_path, xml_gz_path


def decompress_xml_gz(xml_gz_path: PathLike, to_path: PathLike = None) -> str:
    if to_path is None:
        fd, to_path = tempfile.mkstemp("dblp.xml", "conch")
        os.close(fd)
    with gzip.open(xml_gz_path, 'rb') as fr:
        with open(to_path, 'wb') as fw:
            chunk_size = int(conf['network']['chunk_size'])
            while True:
                chunk = fr.read(chunk_size)
                if chunk:
                    fw.write(chunk)
                else: break
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

        return {
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


@app.task(name="streamin.analyze_dblp")
def analyze_dblp(dtd_path: PathLike, xml_gz_path: PathLike):
    last_ended = r.get('dblp_last_analyze_ended')
    if not last_ended:
        logger.error("The last dblp dump task has not finished yet")
        return

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
            parent = elem.getparent()
            if parent:
                parent.clear()
            elem.clear()
        else:
            logger.error(f"unknown xml sax event: {event}")

    os.remove(xml_gz_path)
    os.remove(xml_path)


def match_action(type: Literal["article", "inproceedings", "homepage"],
                 key: str, mdate: str) -> Optional[Literal["insert", "update"]]:
    logged_key = f'dblp_{type}_{key}'
    cached_mdate = r.get(logged_key)
    if cached_mdate is not None:
        last_mdate = cached_mdate.decode()
    else:
        dblp_item = t_dblp.find_one({'key': logged_key})
        if dblp_item is None:
            return "insert"
        last_mdate = dblp_item['mdate']

    if mdate != last_mdate:
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

    # rename author.key to author.streamin_key
    for author in doc['authors']:
        author['streamin_key'] = author['key']
        del author['key']

    del doc['mdate']

    doc['dblp_key'] = doc['key']
    del doc['key']

    return doc


@app.task(name="streamin.process_record")
def process_record(info: Dict):
    assert info['type'] in ['article', 'inproceedings']

    type, key, mdate = info['type'], info['key'], info['mdate']
    action = match_action(type, key, mdate)

    if action:
        logged_key = f'dblp_{type}_{key}'
        r.set(logged_key, mdate)

        doc = translate_record(info)

        task_name = "records.insert" if action == 'insert' else "records.update"
        app.send_task(task_name, kwargs=doc)

        t_dblp.update_one(
            {'key': logged_key}, {'$set': {'mdate': mdate}}, upsert=True)
        r.set(logged_key, mdate)


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

    doc['streamin_keys'] = doc['names']

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


@app.task(name="streamin.process_homepage")
def process_homepage(info: Dict):
    assert info['type'] == 'homepage'

    type, key, mdate = info['type'], info['key'], info['mdate']
    action = match_action(type, key, mdate)

    if action:
        logged_key = f'dblp_{type}_{key}'
        r.set(logged_key, mdate)

        doc = translate_homepage(info)

        task_name = "authors.insert" if action == 'insert' else "authors.update"
        app.send_task(task_name, kwargs=doc)

        t_dblp.update_one(
            {'key': logged_key}, {'$set': {'mdate': mdate}}, upsert=True)
        r.set(logged_key, mdate)
