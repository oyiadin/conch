# coding=utf-8
from typing import List, Dict, Literal
import urllib.parse

import requests

from conch.conch_authors import *


def _get_update_operations__clever(document: Dict, **updates) -> Dict:
    """Update the very article cleverly. Returns the MongoDB set operation.
    1. update the previously blank fields"""

    operations = {}

    # process the special fields first
    if 'abstract' in updates:
        # if the original abstract is blank
        if 'abstract' not in document or not document['abstract']['value']:
            from_scratch = 'abstract' not in document

            abstract = updates['abstract']
            if from_scratch:
                operations['abstract'] = {'value': abstract}
            else:
                operations['abstract.value'] = abstract
            hash_parts = utils.get_hash_parts(utils.simhash(abstract))
            for index in range(4):
                if from_scratch:
                    operations['abstract'][f'simhash{index+1}'] = hash_parts[index]
                else:
                    operations[f"abstract.simhash{index+1}"] = hash_parts[index]
        del updates['abstract']  # delete from the update request

    # then process the normal fields
    for key, value in updates.items():
        if key not in document or not document[key]:
            operations[key] = value

    return operations


@app.task(name="authors.update_or_insert")
def update_or_insert(
        urls: List[Dict[Literal["type", "text"], str]] = None,
        affiliations: List[Dict[Literal["label", "text"], str]] = None,
        awards: List[Dict[Literal["label", "text"], str]] = None,
        uname: str = None,
        dblp_homepage: str = None,
        is_disambiguation: bool = None,
        streamin_keys: List[str] = None,
        names: List[str] = None,
        orcid: str = None):
    # append orcid
    if streamin_keys:
        for streamin_key in streamin_keys:
            result = t_authors.find_one({'streamin_keys': streamin_key})
            if result:
                logger.info(f"author of streamin_key {streamin_key} already existed")
                operations = _get_update_operations__clever(
                    result,
                    urls=urls,
                    affiliations=affiliations,
                    awards=awards,
                    uname=uname,
                    is_disambiguation=is_disambiguation,
                    names=names,
                    orcid=orcid,
                )
                t_authors.update_one(
                    {'_id': result['_id']}, {'$set': operations})
                return

    document = {
        "names": names,
        "uname": uname,
        "orcid_name": {
            "given_names": "",
            "family_name": "",
        },
        "affiliations": affiliations,
        "urls": urls,
        "awards": awards,
        "biography": "",
        "dblp_homepage": dblp_homepage,
        "is_disambiguation": is_disambiguation,
        "streamin_keys": streamin_keys,
        "orcid": [orcid],
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
            document['orcid_name']['given_names'] = name['given-names']['value']
        if name.get('family-name') is not None:
            document['orcid_name']['family_name'] = name['family-name']['value']
        if data.get('biography') and 'content' in data['biography']:
            document['biography'] = data['biography']['content']

    result = t_authors.insert_one(document)
    return result.inserted_id
