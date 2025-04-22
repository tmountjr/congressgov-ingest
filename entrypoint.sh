#!/bin/bash

# Mount Google Cloud Storage bucket
if [ "$TARGETARCH" = "amd64" ]; then
  gcsfuse --implicit-dirs cta-data /ingest/data
fi

exec python main.py