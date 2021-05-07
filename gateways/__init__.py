# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database
import redis
from fastapi import FastAPI


app = FastAPI()

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

sh = logging.StreamHandler()
sh.setLevel(conf['log']['level'])
logger = logging.getLogger(__name__)
logger.setLevel(conf['log']['level'])
logger.addHandler(logging.StreamHandler())

celery_app = celery.Celery("gateways", broker=conf['mq']['url'])
celery_app.config_from_object('celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_authors = db['authors']  # type: pymongo.database.Collection
t_records = db['records']  # type: pymongo.database.Collection
t_users = db['users']  # type: pymongo.database.Collection

r = redis.Redis(host=conf['redis']['host'],
                port=conf['redis']['port'],
                db=conf['redis']['db'])
