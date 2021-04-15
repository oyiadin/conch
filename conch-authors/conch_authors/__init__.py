# coding=utf-8

import configparser
import logging

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

