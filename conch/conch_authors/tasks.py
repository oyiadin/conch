# coding=utf-8
from typing import List, Dict, Literal
import urllib.parse

import requests

from conch.conch_authors import *


@app.task(name="authors.update_or_insert")
def update_or_insert(
        urls: List[Dict[Literal["type", "text"], str]] = None,
        affiliations: List[Dict[Literal["label", "text"], str]] = None,
        awards: List[Dict[Literal["label", "text"], str]] = None,
        uname: str = None,
        dblp_homepage: str = None,
        is_disambiguation: bool = None,
        streamin_key: str = None,
        full_name: str = None,
        other_names: List[str] = None,
        orcid: str = None):
    # append orcid
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
