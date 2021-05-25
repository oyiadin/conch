FROM python:3.9-alpine

WORKDIR /proj/

COPY requirements.txt ./
RUN pip install -r requirements.txt

EXPOSE 8080

COPY *.py ./gateways/
COPY config.ini ./

ENTRYPOINT [ "uvicorn", "gateways.main:app", "--root-path", "/api" ]
CMD [ "--host", "0.0.0.0", "--port", "8080" ]
