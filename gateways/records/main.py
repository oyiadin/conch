# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database
from fastapi import FastAPI, HTTPException
from marshmallow import EXCLUDE

from .schemas import OutputRecordSchema

app = FastAPI()

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

sh = logging.StreamHandler()
sh.setLevel(conf['log']['level'])
logger = logging.getLogger(__name__)
logger.setLevel(conf['log']['level'])
logger.addHandler(logging.StreamHandler())

celery_app = celery.Celery("gateways.records", broker=conf['mq']['url'])
celery_app.config_from_object('celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_records = db['records']  # type: pymongo.database.Collection


@app.get("/dblp_key/{key:path}")
async def find_record_via_dblp_key(key: str):
    schema = OutputRecordSchema(partial=True, context={'to': 'api'})
    record = t_records.find_one({'dblp_key': key})
    if record is None:
        raise HTTPException(status_code=404, detail="No such record")
    return schema.dump(schema.load(record, unknown=EXCLUDE))


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app,
                host=conf['server']['host'],
                port=int(conf['server']['port']))
