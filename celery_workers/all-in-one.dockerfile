FROM python:3.9

ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt ./celery_workers/
RUN pip install -r ./celery_workers/requirements.txt

COPY authors/requirements.txt ./celery_workers/authors/
RUN pip install -r ./celery_workers/authors/requirements.txt

COPY datafeeder/requirements.txt ./celery_workers/datafeeder/
RUN pip install -r ./celery_workers/datafeeder/requirements.txt

COPY records/requirements.txt ./celery_workers/records/
RUN pip install -r ./celery_workers/records/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY datafeeder/*.py datafeeder/config.ini ./celery_workers/datafeeder/
COPY records/*.py records/config.ini ./celery_workers/records/
COPY authors/*.py authors/config.ini ./celery_workers/authors/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.all_in_one_tasks", \
    "worker", \
    "-Q", "conch_datafeeder", \
    "-Q", "conch_records", \
    "-Q", "conch_authors", \
    "-l", "INFO" \
]
