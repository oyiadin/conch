FROM python:3.9

ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt ./celery_workers/
RUN pip install -r ./celery_workers/requirements.txt

COPY datafeeder/requirements.txt ./celery_workers/datafeeder/
RUN pip install -r ./celery_workers/datafeeder/requirements.txt

COPY recommender/requirements.txt ./celery_workers/recommender/
RUN pip install -r ./celery_workers/recommender/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY datafeeder/*.py datafeeder/config.ini ./celery_workers/datafeeder/
COPY recommender/*.py recommender/config.ini ./celery_workers/recommender/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.all_in_one_tasks", \
    "worker", \
    "-Q", "conch_datafeeder", \
    "-Q", "conch_recommender", \
    "-l", "INFO" \
]
