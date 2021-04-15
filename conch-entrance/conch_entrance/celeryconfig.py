# coding=utf-8

from celery.schedules import crontab

time_zone = 'UTC'

task_annotations = {
    'streamin.fetch_and_analyse_dblp_dump': {'rate_limit': '2/m'},
    'streamin.*': {'queue': 'streamin'},
}

beat_schedule = {
    'dblp-daily-fetch': {
        'task': 'streamin.fetch_and_analyse_dblp_dump',
        'schedule': crontab(hour='4,16', minute=0)
    }
}
