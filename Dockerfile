FROM python:3.9

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/proj
WORKDIR /proj

COPY conch/requirements.txt conch/
RUN pip install -r conch/requirements.txt

COPY conch/*.py conch/
COPY conch/config.ini conch/

ENTRYPOINT [ "celery", "-A", "conch", "beat", "-l", "info" ]
