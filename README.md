# Congress.gov Ingest

This project is part of a larger effort to create a data pipeline around [the @unitedstates congress](https://github.com/unitedstates/congress) scraper utility. This is not meant to be used in a standalone fashion.

## Intended Use

This app was designed to run after the `unitedstates/congress` repo has been cloned into a sibling directory:

```
your-app/
├── congress
├── congressgov-ingest
└── update.sh
```

The contents of `update.sh` can be whatever you need for your requirements, but this is what I'm using:

```bash
#!/usr/bin/env sh

cd congress

# This assumes you have already created a virtualenv and installed the dependencies.
source env/bin/activate

# Get the votes first.
usc-run votes --congress=119

# Then update govinfo bulk data.
usc-run govinfo --bulkdata=BILLS,BILLSTATUS --congress=119

# Finally generate necessary data.json files.
usc-run bills --congress=119

# Copy the data to the ingest app.
cp -r data ../congressgov-ingest/data

# Leave the virtual environment.
deactivate

# Now switch to the ingest project and update the database.
cd ../congressgov-ingest
source env/bin/activate

# Delete the database file if it's found.
if [ -f "congressgov.db" ]; then
  rm congressgov.db
fi

# Grab the latest legislator JSON.
curl -o data/legislators-current.json https://unitedstates.github.io/congress-legislators/legislators-current.json

# Run the main script to populate the database.
python main.py

# Leave the virtual environment.
deactivate

# Move the database file to the parent directory.
mv congressgov.db ../

# Back to the previous directory and we're done.
cd ..
```

## Installation

1. Set up a single directory (eg. `your-app`) to house both projects.
1. Clone https://github.com/unitedstates/congress into `your-app`.
1. Navigate into the `congress` directory, create a virtual environment (eg. `python3 -m venv env`), and activate it (`source env/bin/activate`).
1. Install dependencies (`pip install .`).
1. Back out to `your-app` and clone this repo.
1. Navigate into the `congressgov-ingest` directory, create a virtual environment (as above), and activate it (as above).
1. Back out again to `your-app` and run `update.sh`. WARNING: This step can take a substantial amount of time the first time it runs. Subsequent downloads will be faster.

Once the update script has completed, you should see `congressgov.db` in the root of `your-app`. This SQLite database is portable and ready to be queried.

## Standalone operation

Once the `congressgov-ingest` setup is complete and you have all the downloaded data, you can run `main.py` from within the `congressgov-ingest` directory and generate the `congressgov.db` file directly in the current directory.

## Acknowledgements

This project is built on what must have been a herculean amount of effort on the part of the team behind [the @unitedstates project](https://unitedstates.github.io/). They built and maintain the scraper that is collecting all this information.