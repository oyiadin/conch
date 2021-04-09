# coding=utf-8

import configparser
import logging
from typing import List, Tuple

import celery
import pymongo
import pymongo.database


logger = logging.getLogger("conch-streamin")

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

app = celery.Celery("conch.streamin", broker=conf['mq']['url'])

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_authors = db['authors']  # type: pymongo.database.Collection


@app.task
def analyse_arxiv_database():
    pass


@app.task
def analyse_dblp_dump():
    pass


@app.task
def crawl_arxiv():
    pass
