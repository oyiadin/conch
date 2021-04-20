# coding=utf-8

time_zone = 'UTC'

task_annotations = {
    "authors.append_orcid": {"rate_limit": '22/s'},  # orcid limits
}

task_default_queue = 'celery_workers'
task_routes = {
    'datafeeder.*': {'queue': 'datafeeder'},
    'records.*': {'queue': 'records'},
    'authors.*': {'queue': 'authors'},
}
