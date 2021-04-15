# coding=utf-8

import requests

from conch_streamin import *
from conch_streamin.dblp import dblp_analyze_entrance


@app.task(name="streamin.analyse_arxiv_database")
def analyse_arxiv_database():
    pass


@app.task(name="streamin.fetch_and_analyse_dblp_dump")
def fetch_and_analyse_dblp_dump():
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
            logger.debug("start fetch and analyze dblp dump")
            dblp_analyze_entrance()
            r.set('dblp_last_etag', etag)

    r.mset({
        'dblp_last_dump_started': '0',
        'dblp_last_dump_ended': '1',
    })


@app.task(name="streamin.crawl_arxiv")
def crawl_arxiv():
    pass
