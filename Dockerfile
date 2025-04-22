FROM python:3-slim

# Install gcsfuse to enable mounting gcs as a volume.
# RUN if [ $TARGETARCH = "amd64" ]; then \
#   apt-get update && apt-get install -y fuse gcsfuse && rm -rf /var/lib/apt/lists/* \
# ; fi

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /ingest
COPY ./requirements.txt /ingest/
RUN pip install --no-cache-dir -r requirements.txt
COPY ./database /ingest/database
COPY ./main.py /ingest/
RUN mkdir /ingest/data

ENTRYPOINT [ "python", "main.py" ]

# Usage:
# docker run \
#   --entrypoint python main.py \
#   --mount type=bind,src=/local/path/to/data,dst=/ingest/data \
#   tmountjr/cta-ingest:latest