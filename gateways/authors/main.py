# coding=utf-8

import configparser
import logging
from typing import List, Optional, Dict

import celery
import pymongo
import pymongo.database
from fastapi import FastAPI, HTTPException
from marshmallow import EXCLUDE

from .schemas import OutputAuthorSchema


app = FastAPI()

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

sh = logging.StreamHandler()
sh.setLevel(conf['log']['level'])
logger = logging.getLogger(__name__)
logger.setLevel(conf['log']['level'])
logger.addHandler(logging.StreamHandler())

celery_app = celery.Celery("gateways.authors", broker=conf['mq']['url'])
celery_app.config_from_object('celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_authors = db['authors']  # type: pymongo.database.Collection


def find_author(datafeeder_keys: List[str],
                raise_exc: bool = False) -> Optional[Dict]:
    assert len(datafeeder_keys), "Empty datafeeder_keys is not allowed"
    query = {'$or': [{'datafeeder_keys': key} for key in datafeeder_keys]}
    result = t_authors.find_one(query)
    if result is None and raise_exc:
        raise HTTPException(
            status_code=404,
            detail="No such author with datafeeder_keys: %s" % datafeeder_keys)
    return result


@app.get("/datafeeder_key/{key}")
async def find_author_via_datafeeder_key(key: str):
    schema = OutputAuthorSchema(unknown=EXCLUDE, partial=True)
    author = find_author([key], raise_exc=True)
    return schema.dump(schema.load(author))


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app,
                host=conf['server']['host'],
                port=int(conf['server']['port']))
