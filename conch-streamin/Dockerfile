FROM python:3.9

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/conch
WORKDIR /conch

COPY requirements.txt /conch
RUN pip install -r requirements.txt

COPY config.ini /conch
COPY conch_streamin/ /conch/conch_streamin/

CMD python conch_streamin/dblp.py
