import os
import re
import json
from pathlib import Path

def count_votes(congress_num, data_dir):
    """
    Count the number of votes for a given Congress session.
    """
    # Enumerate [data_dir]/[congress_num]/votes/**/data.json (ie. count the number
    # of data.json files) and return that value.
    path = Path(os.path.join(data_dir, str(congress_num), "votes"))
    return sum(1 for file in path.rglob("data.json"))

def count_legislators(data_dir):
    """
    Count the number of legislators that should have been imported.
    """
    path = Path(data_dir)
    # Import [path]/legislators.json and return the number of entries.
    with open(path / "legislators.json", "r", encoding="utf-8") as f:
        data = json.loads(f.read())
        return len(data)

def downloaded_sessions(data_dir):
    """Return a List of the sessions for which we have metadata."""
    path = Path(data_dir)
    return [int(d.name) for d in path.iterdir() if d.is_dir() and re.match(r"^\d+$", d.name)]
