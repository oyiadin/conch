FROM conch

COPY requirements.txt conch/conch_records/
RUN pip install -r conch/conch_records/requirements.txt

COPY *.py conch/conch_records/
COPY config.ini conch/conch_records/

ENTRYPOINT [
    "celery",
    "-A", "conch.conch_records.tasks",
    "worker",
    "-Q", "conch_records",
    "-n", "conch_records",
    "-l", "info"
]
