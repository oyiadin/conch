# coding=utf-8

import configparser
import os

import celery
import pymongo
import pymongo.database
from loguru import logger


conf = configparser.ConfigParser()
conf.read_file(open("conch/config.ini"))
conf.read_file(open("conch/conch_records/config.ini"))

app = celery.Celery("conch.records", broker=conf['mq']['url'])
app.config_from_object('conch.celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_records = db['records']  # type: pymongo.database.Collection
