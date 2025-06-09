FROM python:3-slim

WORKDIR /ingest
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY database ./database
COPY main.py .
COPY shared_meta.py .
COPY sanity_check.py .
COPY stdout_logger.py .

ENTRYPOINT [ "python", "main.py" ]

# Usage:
# docker run \
#   --mount type=bind,src=/local/path/to/data,dst=/ingest/data \
# tmountjr/cta-ingest:latest --data_dir=/ingest/data
