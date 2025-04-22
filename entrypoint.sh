#!/bin/bash

# Mount Google Cloud Storage bucket
if [ "$TARGETARCH" = "amd64" ]; then
  gcsfuse --implicit-dirs --only-dir data cta-data /ingest/data
fi

exec python main.py