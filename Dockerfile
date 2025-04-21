FROM python:3.13-slim

WORKDIR /ingest
COPY ./requirements.txt /ingest/
RUN pip install --no-cache-dir -r requirements.txt
COPY ./database /ingest/database
COPY ./main.py /ingest/

ENTRYPOINT [ "python", "./main.py" ]

# Usage:
# docker run \
#   --mount type=bind,src=/local/path/to/data,dst=/ingest/data \
#   my-container