# coding=utf-8

import configparser
import logging

import celery
import pymongo
import pymongo.database
import redis
import requests

from conch_streamin.dblp import dblp_analyze_entrance


logger = logging.getLogger("conch-streamin")

conf = configparser.ConfigParser()
conf.read_file(open("config.ini"))

app = celery.Celery("conch.streamin", broker=conf['mq']['url'])

dbclient = pymongo.MongoClient(conf['db']['url'])
db = dbclient[conf['db']['db_name']]  # type: pymongo.database.Database
t_dblp = db['dblp']  # type: pymongo.database.Collection
t_arxiv = db['arxiv']


r = redis.Redis(host=conf['redis']['host'],
                port=conf['redis']['port'],
                db=conf['redis']['db'])

@app.task
def analyse_arxiv_database():
    pass


@app.task
def analyse_dblp_dump():
    last_started = int(r.get('dblp_last_dump_started') or '0')
    last_ended = int(r.get('dblp_last_dump_ended') or '0')
    if last_started and not last_ended:
        logger.error("The last dblp dump task has not finished yet")
        return
    r.mset({
        'dblp_last_dump_started': '1',
        'dblp_last_dump_ended': '0',
    })

    last_etag = r.get('dblp_last_etag')
    with requests.head(conf['dblp']['url']) as response:
        etag = response.headers['ETag']
        if last_etag and etag == last_etag:
            logger.info("dblp etag not changed, stopping dumping")
        else:
            response.close()  # early close
            dblp_analyze_entrance()
            r.set('dblp_last_etag', etag)

    r.mset({
        'dblp_last_dump_started': '0',
        'dblp_last_dump_ended': '1',
    })

@app.task
def crawl_arxiv():
    pass
