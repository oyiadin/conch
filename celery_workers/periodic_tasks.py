# coding=utf-8

import celery
from celery.schedules import crontab

from celery_workers import app

@app.on_after_configure.connect
def setup_periodic_tasks(sender: celery.Celery, **kwargs):
    pass
    # sender.add_periodic_task(
    #     crontab(hour='4,6', minute=0),
    #     celery.signature({'task': 'datafeeder.fetch_dblp'}),
    #     args=(True,)
    # )
