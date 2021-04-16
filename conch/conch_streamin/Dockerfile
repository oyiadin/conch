FROM conch

COPY requirements.txt conch/conch_streamin/
RUN pip install -r conch/conch_streamin/requirements.txt

COPY *.py conch/conch_streamin/
COPY config.ini conch/conch_streamin/

ENTRYPOINT [
    "celery",
    "-A", "conch.conch_streamin.tasks",
    "worker",
    "-Q", "conch_streamin",
    "-n", "conch_streamin",
    "-l", "info"
]
