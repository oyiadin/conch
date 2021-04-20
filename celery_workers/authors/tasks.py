# coding=utf-8
from typing import List, Dict, Literal, Optional
import urllib.parse

import requests
from marshmallow import EXCLUDE

from celery_workers.authors import *
from celery_workers.authors.schemas import AuthorSchema


def find_author(datafeeder_keys: List[str]) -> Optional[Dict]:
    assert len(datafeeder_keys), "Empty datafeeder_keys is not allowed"
    query = {'$or': [{'datafeeder_keys': key} for key in datafeeder_keys]}
    return t_authors.find_one(query)


@app.task(name="authors.insert")
def insert(doc: Dict):
    schema = AuthorSchema()
    loaded = schema.load(doc)
    dumped = schema.dump(loaded)

    assert 'datafeeder_keys' in loaded, f"datafeeder_keys is required when " \
                                        f"inserting author: {dumped}"
    datafeeder_keys = loaded['datafeeder_keys']
    if find_author(datafeeder_keys):
        raise FileExistsError("An author with a same datafeeder_key within %s "
                              "is found in database", datafeeder_keys)

    logger.debug("Inserting a new author into database: %s", str(dumped))
    t_authors.insert_one(dumped)


@app.task(name="authors.append_orcid")
def append_orcid(datafeeder_keys: List[str], orcid: str):
    author_in_db = find_author(datafeeder_keys)
    if author_in_db is None:
        raise FileNotFoundError(
            f"No such author with datafeeder_keys: {datafeeder_keys}")

    orcids_in_db = author_in_db.get('orcids', [])
    for orcid_in_db in orcids_in_db:
        if orcid_in_db['value'] == orcid:
            logger.warning("This author has already a same orcid: %s", orcid)
            return

    url = urllib.parse.urljoin(conf['orcid']['api_url'], f'/{orcid}/person')
    response = requests.get(url, headers={'Accept': 'application/json'})
    orcid_data = response.json()
    if orcid_data.get('response-code', None) == 404:
        logger.error('Error occurred when running append_orcid(%s, %s): %s',
                     datafeeder_keys, orcid, orcid_data['developer-message'])
        return

    push_data = {'value': orcid}

    name = orcid_data['name']
    if name.get('given-names') is not None:
        push_data['given_names'] = name['given-names']['value']
    if name.get('family-name') is not None:
        push_data['family_name'] = name['family-name']['value']
    if orcid_data.get('biography') and 'content' in orcid_data['biography']:
        push_data['biography'] = orcid_data['biography']['content']

    t_authors.update_one({'_id': author_in_db['_id']},
                         {'$push': {'orcids': push_data}})


def translate_to_db_operations(document: Dict, updates: Dict) -> Dict:
    schema = AuthorSchema(only=['names', 'uname', 'affiliations', 'urls',
                                'awards', 'is_disambiguation',
                                'datafeeder_keys'],
                          exclude=['_id', 'dblp_homepage', 'orcids'],
                          unknown=EXCLUDE)
    loaded_document = schema.load(document)
    loaded_updates = schema.load(updates)

    sets, pushes = {}, {}

    for k, v in loaded_updates.items():
        if k in ['names', 'datafeeder_keys']:  # List[String]
            items = list(filter(
                lambda x: x not in loaded_document.get(k, []), v))
            if items:
                pushes[k] = items
        elif k in ['affiliations', 'urls', 'awards']:  # List[Nested Dict]
            key_inside = {'affiliations': 'text',
                          'urls': 'text',
                          'awards': 'text'}[k]  # for future purpose
            pushes[k] = []
            for new_item in v:
                for old_item in loaded_document.get(k, []):
                    if new_item[key_inside] == old_item[key_inside]:
                        break
                else:
                    pushes[k].append(new_item)
            if not pushes[k]:
                del pushes[k]
        else:  # String or Bool
            if v != loaded_document.get(k):
                sets[k] = v

    ret = {}
    if sets:
        ret['$set'] = sets
    if pushes:
        ret['$push'] = {}
        for key, values in pushes.items():
            ret['$push'][key] = {'$each': values}
    return ret


@app.task(name="authors.update")
def update(doc: Dict):
    if 'orcids' in doc:
        logger.error("It's not allowed to update orcids with authors.update, "
                     "call authors.append_orcid instead.")
        return

    schema = AuthorSchema(partial=True)
    loaded = schema.load(doc)

    assert 'datafeeder_keys' in loaded, f"datafeeder_keys is required when " \
                                        f"updating author: {loaded}"
    assert len(loaded['datafeeder_keys']), f"datafeeder_keys must not be " \
                                           f"empty when updating author: " \
                                           f"{loaded}"

    query = {'$or': [{'datafeeder_keys': key}
                     for key in loaded['datafeeder_keys']]}
    author_in_db = t_authors.find_one(query)
    db_operations = translate_to_db_operations(author_in_db, loaded)
    t_authors.update_one(query, db_operations)
