FROM continuumio/miniconda3

ENV PYTHONPATH=/conch
WORKDIR /conch

RUN conda install -c pytorch faiss-cpu

COPY requirements.txt ./celery_workers/
RUN pip install -r ./celery_workers/requirements.txt

COPY recommender/requirements.txt ./celery_workers/recommender/
RUN pip install -r ./celery_workers/recommender/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY recommender/*.py recommender/config.ini ./celery_workers/recommender/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.recommender.tasks", \
    "worker", \
    "-Q", "conch_recommender", \
    "-l", "INFO" \
]
