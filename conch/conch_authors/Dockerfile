FROM conch

COPY requirements.txt conch/conch_authors/
RUN pip install -r conch/conch_authors/requirements.txt

COPY *.py conch/conch_authors/
COPY config.ini conch/conch_authors/

ENTRYPOINT [
    "celery",
    "-A", "conch.conch_authors.tasks",
    "worker",
    "-Q", "conch_authors",
    "-n", "conch_authors",
    "-l", "info"
]
