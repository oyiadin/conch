FROM continuumio/miniconda3

ENV PYTHONPATH=/conch
WORKDIR /conch

RUN /opt/conda/bin/conda install --freeze-installed -y -c pytorch \
    faiss-cpu \
    && /opt/conda/bin/conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete

COPY requirements.txt ./celery_workers/
RUN pip install --no-cache-dir -r ./celery_workers/requirements.txt

COPY recommender/requirements.txt ./celery_workers/recommender/
RUN pip install --no-cache-dir -r ./celery_workers/recommender/requirements.txt

COPY *.py global-config.ini ./celery_workers/

COPY recommender/*.py recommender/config.ini ./celery_workers/recommender/

ENTRYPOINT [ \
    "celery", \
    "-A", "celery_workers.recommender.tasks", \
    "worker", \
    "-Q", "conch_recommender", \
    "-l", "INFO" \
]
