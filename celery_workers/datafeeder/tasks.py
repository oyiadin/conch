# coding=utf-8
import gzip
import json
import os
import tempfile
import time
from typing import Dict, List

from celery_workers.datafeeder import *
from celery_workers.datafeeder.utils import *


def append_paper_to_author(author_id: int, paper_id: str, author_name: str):
    t_authors.update_one({"_id": author_id},
                         {"$push": {"papers": paper_id},
                          "$setOnInsert": {"name": author_name}},
                         upsert=True)


def perform_insert_many_records(records: List[Dict]):
    t_records.insert_many(records, ordered=False)



banned_fields = [
    'Biology', 'Political Science', 'Sociology', 'Business', 'Geography', 'Art',
    'Psychology', 'Environmental Science', 'Philosophy', 'Materials Science',
    'Geology', 'Economics', 'History', 'Chemistry', 'Medicine', 'Physics'
]


@app.task(name="datafeeder.process_s2")
def task_process_s2_urls(paths: List[os.PathLike]):
    for i, filepath in enumerate(paths):
        study_fields = set()
        with gzip.open(filepath, "rb") as f:
            buffered_records = []
            last_time = time.time()
            for j, line in enumerate(f, start=1):
                record = json.loads(line)
                if not len(record['fieldsOfStudy']):
                    continue
                if len(record['fieldsOfStudy']) and \
                        all(map(lambda x: x in banned_fields,
                                record['fieldsOfStudy'])):
                    continue
                del record['entities']
                del record['s2Url']
                del record['s2PdfUrl']
                del record['doiUrl']
                del record['sources']
                record['journalPages'] = record['journalPages'].strip()
                record['_id'] = paper_id = record.pop('id')
                study_fields.update(record['fieldsOfStudy'])
                for author in record['authors']:
                    author_name = author['name']
                    for a_id in author['ids']:
                        append_paper_to_author(int(a_id), paper_id, author_name)

                buffered_records.append(record)
                if len(buffered_records) >= 1000:
                    perform_insert_many_records(buffered_records)
                    buffered_records.clear()

                if j % 10000 == 0:
                    time_diff = time.time() - last_time
                    logger.debug(
                        "Processed %d records in %s (%s / 10000 records)",
                        j,
                        explain_second(time_diff),
                        explain_second(time_diff * 10000 / j))
                    logger.debug("Collected study fields: %s",
                                 ', '.join(study_fields))

            if buffered_records:
                perform_insert_many_records(buffered_records)
                buffered_records.clear()
