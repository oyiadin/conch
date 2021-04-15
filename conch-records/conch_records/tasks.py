# coding=utf-8

from typing import List, Dict
import conch_records.utils as utils

from conch_records import *


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


@app.task
def add(title: str,
        authors: List[Dict] = None,
        abstract: str = None,
        make_unique: bool = True):

    if make_unique:
        similar_article = utils.find_similar(t_articles, title)
        # if make_unique is enabled and we've found a similar article,
        # then just update it cleverly
        if similar_article is not None:
            logger.warning(f"a similar article was found:\n"
                           f" [{title}]\n"
                           f" [{similar_article['title']['value']}")

            update_operations = _get_update_operations__clever(
                similar_article,
                title=title, authors=authors, abstract=abstract
            )
            t_articles.update_one(
                {'_id': similar_article['_id']},
                {'$set': update_operations}
            )
            return

    # if no similar article was found or make_unique was explicitly disabled
    # then we just simply insert it into our database
    t_articles.insert_one(utils.strip_off_blank_values(
        title=title,
        authors=authors,
        abstract=abstract,
    ))
