FROM python:3.13-slim

WORKDIR /ingest
COPY ./requirements.txt /ingest/
RUN pip install -r requirements.txt
COPY ./database /ingest/database
COPY ./main.py /ingest/

ENTRYPOINT [ "python", "./main.py" ]