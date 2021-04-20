# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database
from celery.utils.log import get_task_logger

conf = configparser.ConfigParser()
conf.read_file(open("celery_workers/global-config.ini"))
conf.read_file(open("celery_workers/records/config.ini"))

logger = get_task_logger(__name__)  # type: logging.Logger
logger.setLevel(conf['log']['level'])

app = celery.Celery("celery_workers.records", broker=conf['mq']['url'])
app.config_from_object('celery_workers.celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_records = db['records']  # type: pymongo.database.Collection
