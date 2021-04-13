# coding=utf-8

import configparser
import logging
from typing import List, Tuple, Dict
import urllib.parse

import celery
import pymongo
import pymongo.database
import requests


logger = logging.getLogger("conch-authors")

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

app = celery.Celery("conch.authors", broker=conf['mq']['url'])

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_authors = db['authors']  # type: pymongo.database.Collection


@app.task
def add(full_name: str,
        dblp_key: str,
        other_names: List[str] = None,
        urls: List[str] = None,
        affiliations: List[Dict] = None,
        orcid: str = None):
    result = t_authors.find_one({'keys.dblp': dblp_key})
    if result:
        logger.info(f"author of dblp_key {dblp_key} already existed")
        return

    # filter out the useless or repeated urls
    urls = list(filter(lambda x: '://orcid.org/' not in x, urls))

    document = {
        'keys': {
            'dblp': dblp_key,
            'orcid': orcid
        },
        'name': {
            'full_name': full_name,
            'given_names': None,
            'family_name': None,
            'other_names': other_names
        },
        'bio': None,
        'urls': urls,
        'affiliations': [
            {
                'name': item['name'],
                'label': item['label'],
            } for item in affiliations or []
        ],
    }

    if orcid:
        url = urllib.parse.urljoin(conf['orcid']['api_url'], f'/{orcid}/person')
        response = requests.get(url, headers={'Accept': 'application/json'})
        data = response.json()
        if data.get('response-code', None) == 404:
            logger.error(data['developer-message'])
            return

        name = data['name']
        if name.get('given-names') is not None:
            document['name']['given_names'] = name['given-names']['value']
        if name.get('family-name') is not None:
            document['name']['family_name'] = name['family-name']['value']
        if data.get('biography') and 'content' in data['biography']:
            document['bio'] = data['biography']['content']

    result = t_authors.insert_one(document)
    return result.inserted_id
