FROM python:3.9

ENV PYTHONUNBUFFERED=1
WORKDIR /conch_streamin

COPY requirements.txt /conch_streamin
RUN pip install -r requirements.txt
COPY . /conch_streamin
