# coding=utf-8

from celery.schedules import crontab


time_zone = 'UTC'

task_annotations = {
    'streamin.fetch_and_analyse_dblp_dump': {'rate_limit': '200/m'},
}

task_default_queue = 'conch'
task_routes = {
    'streamin.*': {'queue': 'conch_streamin'},
    'records.*': {'queue': 'conch_records'},
    'authors.*': {'queue': 'conch_authors'},
}

beat_schedule = {
    'dblp-daily-fetch': {
        'task': 'streamin.fetch_and_analyse_dblp_dump',
        'schedule': crontab(hour='4,16', minute=0)
    }
}