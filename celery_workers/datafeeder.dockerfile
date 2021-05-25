FROM python:3.9

ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt ./celery_workers/
RUN pip install --no-cache-dir -r ./celery_workers/requirements.txt

COPY datafeeder/requirements.txt ./celery_workers/datafeeder/
RUN pip install --no-cache-dir -r ./celery_workers/datafeeder/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY datafeeder/*.py datafeeder/config.ini ./celery_workers/datafeeder/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.datafeeder.tasks", \
    "worker", \
    "-Q", "conch_datafeeder", \
    "-l", "INFO" \
]
