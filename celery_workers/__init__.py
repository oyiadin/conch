# coding=utf-8

import configparser

import celery


conf = configparser.ConfigParser()
conf.read_file(open("celery_workers/global-config.ini"))

app = celery.Celery("celery_workers.all_in_one_tasks",
                    backend=f"redis://{conf['redis']['host']}"
                            f":{conf['redis']['port']}"
                            f"/{conf['redis']['db']}",
                    broker=conf['mq']['url'])
app.config_from_object('celery_workers.celeryconfig')
