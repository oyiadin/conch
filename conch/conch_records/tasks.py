# coding=utf-8

from typing import Dict, Literal, Optional

from bson import ObjectId

import conch.conch_records.utils as utils

from conch.conch_records import *
from conch.conch_records.schemas import *
from conch.conch_records.stringsimhash import StringSimhash


@app.task(name='records.insert')
def insert(doc: Dict):
    schema = RecordSchema(context={'to': 'db'})
    record = schema.load(doc)
    dumped = schema.dump(record)

    if 'dblp_key' not in dumped:
        raise ValueError("dblp_key is required when inserting record: %s",
                         str(dumped))

    if t_records.find_one({"dblp_key": dumped['dblp_key']}):
        raise FileExistsError(
            "A record with the same dblp_key %s is found in database",
            dumped['dblp_key'])

    logger.debug('Inserting a new record into database: %s', str(dumped))
    t_records.insert_one(dumped)


def translate_to_db_operations(document: Dict, updates: Dict) -> Dict:
    schema = RecordSchema(only=['title', 'authors', 'booktitle', 'volume',
                                'doi', 'ees', 'year', 'pages', 'notes',
                                'abstract'],
                          context={'to': 'db'})
    loaded_document = schema.load(document)
    loaded_updates = schema.load(updates)

    sets, pushes = {}, {}

    for k, v in loaded_updates.items():
        if k in ['title', 'abstract']:  # StringSimhash
            if str(v) != str(loaded_document.get('title', '')):
                sets['title'] = v.to_dict()
        elif k in ['authors', 'ees', 'notes']:  # List
            if k == 'authors' or k == 'notes':  # Dict nested
                key_inside = {'authors': 'streamin_key',
                              'notes': 'text'}[k]
                pushes[k] = []
                for new_author in v:
                    for old_author in document.get(k, []):
                        if (new_author[key_inside]
                                == old_author[key_inside]):
                            break
                    else:
                        pushes[k].append(new_author)
                if not pushes[k]:
                    del pushes[k]
            elif k == 'ees':
                ees = list(filter(
                    lambda x: x not in document.get('ees', []), v))
                if ees:
                    pushes['ees'] = ees
        else:  # String
            if v != document.get(k):
                sets[k] = v

    ret = {}
    if sets:
        ret['$set'] = sets
    if pushes:
        ret['$push'] = pushes
    return ret


def find_similar_record(according_to: str, target_value: StringSimhash,
                        distance_tolerance: int = 3) -> Optional[Dict]:
    schema = RecordSchema()
    similar_records = t_records.find({
        '$or': [{f'{according_to}.simhash{n}': target_value} for n in range(4)]
    })
    for record in similar_records:
        loaded_record = schema.load(record)
        if target_value - loaded_record[according_to] < distance_tolerance:
            return loaded_record

    return None


def update_if_found(similar_field: Literal["abstract", "title"],
                    record: Dict, dumped: Dict) -> bool:
    if similar_field in record and record[similar_field]:
        logger.debug(f"Finding similar records with {similar_field}: "
                     f"{record['abstract'][:64]}")
        similar_record = find_similar_record(similar_field,
                                             record[similar_field])
        if similar_record is not None:
            logger.debug(f"A similar record was found according to "
                         f"{similar_field}: {similar_record['dblp_key']} - "
                         f"{similar_record[similar_field]!s}")
            db_operations = translate_to_db_operations(similar_record, dumped)
            t_records.update_one({'_id': ObjectId(similar_record['_id'])},
                                 db_operations)
            return True
    return False


@app.task(name='records.update')
def update(doc: Dict):
    schema = RecordSchema(partial=True, context={'to': 'db'})
    record = schema.load(doc)
    dumped = schema.dump(record)

    if 'dblp_key' in record and record['dblp_key']:
        query = {'dblp_key': record['dblp_key']}
        record_in_db = t_records.find_one(query)
        assert record_in_db is not None, \
            "Illegal dblp_key {} when running records.update".format(
                record['dblp_key'])
        db_operations = translate_to_db_operations(record_in_db, dumped)
        t_records.update_one(query, db_operations)

    else:
        # find similar articles according to its abstract or title
        is_updated = update_if_found(similar_field='abstract',
                                     record=record, dumped=dumped)
        if not is_updated:
            is_updated = update_if_found(similar_field='title',
                                         record=record, dumped=dumped)

        assert is_updated, f"No any records found in the database with " \
                           f"similar abstract or title: " \
                           f"<Title {record.get('title', '')[:20]}> " \
                           f"<Abstract {record.get('abstract', '')[:20]}>"
