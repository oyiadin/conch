FROM python:3.9

ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt ./celery_workers/
RUN pip install -r ./celery_workers/requirements.txt

COPY records/requirements.txt ./celery_workers/records/
RUN pip install -r ./celery_workers/records/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY records/*.py records/config.ini ./celery_workers/records/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.records.tasks", \
    "worker", \
    "-Q", "conch_records", \
    "-l", "INFO" \
]
