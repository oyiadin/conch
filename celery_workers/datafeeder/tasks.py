# coding=utf-8
import glob
import gzip
import json
import os
import time
from typing import Dict, List, Optional

import celery
from pymongo import UpdateOne

from celery_workers.datafeeder import *
from celery_workers.datafeeder.utils import *


@app.task(name="datafeeder.process_s2")
def task_process_s2(paths: Optional[List[os.PathLike]] = None,
                    pattern: Optional[str] = None,
                    run_next_task: bool = False):
    assert paths or pattern
    if paths is None:
        paths = glob.glob(os.path.join('/data/s2', pattern))

    for i, filepath in enumerate(paths):
        with gzip.open(filepath, "rb") as f:
            buffered_records = []
            buffered_author_papers = {}
            with dbclient.start_session(causal_consistency=True) as session:
                last_time = time.time()
                for line in f:
                    if b'Computer Science' not in line:
                        continue
                    record = json.loads(line)
                    del record['entities']
                    del record['s2Url']
                    del record['s2PdfUrl']
                    del record['doiUrl']
                    del record['sources']
                    record['journalPages'] = record['journalPages'].strip()
                    record['_id'] = paper_id = record.pop('id')
                    for author in record['authors']:
                        author_name = author['name']
                        for a_id in author['ids']:
                            id = int(a_id)
                            if id not in buffered_author_papers:
                                buffered_author_papers[id] = \
                                    (author_name, [paper_id])
                            else:
                                buffered_author_papers[id][1].append(paper_id)

                    buffered_records.append(record)
                    if len(buffered_records) >= 1000:
                        t_records.insert_many(buffered_records,
                                              ordered=False, session=session)
                        buffered_records.clear()
                        t_authors.bulk_write([
                            UpdateOne({"_id": k},
                                      {"$push": {"papers": {"$each": v[1]}},
                                       "$setOnInsert": {"name": v[0]}},
                                      upsert=True)
                            for k, v in buffered_author_papers.items()
                        ], ordered=False, session=session)
                        buffered_author_papers.clear()

                time_diff = time.time() - last_time
                logger.debug(
                    "[%d/%d] Processed %s file in %s",
                    i + 1, len(paths), filepath, explain_second(time_diff))

                if buffered_records:
                    t_records.insert_many(buffered_records,
                                          ordered=False, session=session)
                    buffered_records.clear()
                    t_authors.bulk_write([
                        UpdateOne({"_id": k},
                                  {"$push": {"papers": {"$each": v[1]}},
                                   "$setOnInsert": {"name": v[0]}},
                                  upsert=True)
                        for k, v in buffered_author_papers.items()
                    ], ordered=False, session=session)
                    buffered_author_papers.clear()

    if run_next_task:
        app.send_task('recommender.process_database').forget()


@app.task(name="proxy_recommender.process_database")
def task_proxy_recommender_process_database(*args, **kwargs):
    app.send_task('recommender.process_database').forget()


@app.task(name="datafeeder.distributed_process_s2_then_train_model")
def task_distributed_process_s2_then_train_model():
    callback = task_proxy_recommender_process_database.s()
    header = [task_process_s2.s(pattern='*%d.gz' % i) for i in range(10)]
    result = celery.chord(header)(callback)

