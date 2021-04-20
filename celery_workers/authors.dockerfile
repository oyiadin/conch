FROM python:3.9

ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt ./celery_workers/
RUN pip install -r ./celery_workers/requirements.txt

COPY authors/requirements.txt ./celery_workers/authors/
RUN pip install -r ./celery_workers/authors/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY authors/*.py authors/config.ini ./celery_workers/authors/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.authors.tasks", \
    "worker", \
    "-Q", "conch_authors", \
    "-l", "INFO" \
]
