# coding=utf-8

from typing import List, Dict, Literal
import conch.conch_records.utils as utils

from conch.conch_records import *


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


@app.task(name='records.update_or_insert')
def update_or_insert(
        type: Literal["article", "inproceedings"] = None,
        title: str = None,
        authors: List[Dict[Literal["streamin_key", "orcid"], str]] = None,
        dblp_key: str = None,
        booktitle: str = None,
        journal: str = None,
        volume: str = None,
        doi: str = None,
        ees: List[str] = None,
        year: str = None,
        pages: str = None,
        notes: str = None,
        abstract: str = None):

    if not dblp_key:  # then find similar articles and (maybe) update it
        logger.debug(f'finding similar records with title {title}')
        similar_article = utils.find_similar(t_records, title)
        # if make_unique is enabled and we've found a similar article,
        # then just update it cleverly
        if similar_article is not None:
            logger.debug(f"a similar article was found:\n"
                           f" [{title}]\n"
                           f" [{similar_article['title']['value']}")

            update_operations = _get_update_operations__clever(
                similar_article,
                # only the fields below are allowed to be updated
                title=title,
                booktitle=booktitle,
                journal=journal,
                volume=volume,
                doi=doi,
                ees=ees,
                year=year,
                pages=pages,
                notes=notes,
                abstract=abstract,
            )
            logger.debug('update operations: ' + str(update_operations))
            t_records.update_one(
                {'_id': similar_article['_id']},
                {'$set': update_operations}
            )
            return

    data = {
        'type': type,
        'title': title,
        'authors': authors,
        'dblp_key': dblp_key,
        'booktitle': booktitle,
        'journal': journal,
        'volume': volume,
        'doi': doi,
        'ees': ees,
        'year': year,
        'pages': pages,
        'notes': notes,
        'abstract': abstract,
    }
    logger.debug('inserting a new record in the db: ' + str(data))
    t_records.insert_one(data)
