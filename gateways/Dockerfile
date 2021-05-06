FROM python:3.9

WORKDIR /proj/

COPY requirements.txt ./
RUN pip install -r requirements.txt

EXPOSE 8080

COPY *.py ./gateways/
COPY config.ini ./

ENTRYPOINT [ "uvicorn", "gateways.main:app" ]
CMD [ "--host", "0.0.0.0", "--port", "8080" ]
