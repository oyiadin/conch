# coding=utf-8

from celery_workers import app

app.autodiscover_tasks(['celery_workers.datafeeder'])
app.autodiscover_tasks(['celery_workers.recommender'])
