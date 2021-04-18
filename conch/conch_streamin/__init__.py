# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database
import redis
from celery.utils.log import get_task_logger

conf = configparser.ConfigParser()
conf.read_file(open("conch/config.ini"))
conf.read_file(open("conch/conch_streamin/config.ini"))

logger = get_task_logger(__name__)  # type: logging.Logger
logger.setLevel(conf['log']['level'])

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_dblp = db['dblp']  # type: pymongo.database.Collection
t_arxiv = db['arxiv']  # type: pymongo.database.Collection


r = redis.Redis(host=conf['redis']['host'],
                port=conf['redis']['port'],
                db=conf['redis']['db'])

app = celery.Celery("conch_streamin", broker=conf['mq']['url'])
app.config_from_object('conch.celeryconfig')
