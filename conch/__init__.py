# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database

conf = configparser.ConfigParser()
conf.read_file(open("conch/config.ini"))

app = celery.Celery("conch.entrance", broker=conf['mq']['url'])
app.config_from_object('conch.celeryconfig')

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
