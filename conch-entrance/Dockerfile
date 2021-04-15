FROM python:3.9

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt /conch
RUN pip install -r requirements.txt

COPY config.ini /conch
COPY conch_entrance/ /conch/conch_entrance/

CMD celery -A conch_entrance beat -l INFO
