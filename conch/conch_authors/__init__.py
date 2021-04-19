# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database
from celery.utils.log import get_task_logger


conf = configparser.ConfigParser()
conf.read_file(open("conch/config.ini"))
conf.read_file(open("conch/conch_authors/config.ini"))

logger = get_task_logger(__name__)  # type: logging.Logger
logger.setLevel(conf['log']['level'])

app = celery.Celery("conch.authors", broker=conf['mq']['url'])
app.config_from_object('conch.celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_authors = db['authors']  # type: pymongo.database.Collection
