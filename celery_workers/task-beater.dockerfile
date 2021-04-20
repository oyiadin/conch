FROM python:3.9

ENV PYTHONPATH=/conch
WORKDIR /conch

RUN pip install celery

COPY __init__.py celeryconfig.py periodic_tasks.py global-config.ini \
    ./celery_workers/

ENTRYPOINT [ "celery", "-A", "celery_workers.periodic_tasks", "beat", "-l", "INFO" ]
