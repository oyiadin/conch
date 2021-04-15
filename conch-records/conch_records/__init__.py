# coding=utf-8

import configparser

import celery
import pymongo
import pymongo.database
from loguru import logger


conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

app = celery.Celery("conch.articles", broker=conf['mq']['url'])

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_records = db['records']  # type: pymongo.database.Collection

