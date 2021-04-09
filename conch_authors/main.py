# coding=utf-8

import configparser
import logging
from typing import List, Tuple

import celery
import pymongo
import pymongo.database


logger = logging.getLogger("conch-authors")

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

app = celery.Celery("conch.authors", broker=conf['mq']['url'])

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_authors = db['authors']  # type: pymongo.database.Collection


@app.task
def add(full_name: str, orcid: str):
    result = t_authors.find_one({'orcid': orcid})
    if result:
        logger.warning(f"orcid {orcid} already existed")
        return

    # TODO: integrate with ORCID

    t_authors.insert_one(dict(
        full_name=full_name,
        orcid=orcid,
    ))


@app.task
def multi_add(items: List[Tuple[str, str]]):
    for item in items:
        name, orcid = item
        add(name, orcid)