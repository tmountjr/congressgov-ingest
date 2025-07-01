"""Define base methods for working with the database."""

import os
import json
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base


# Declarative base
Base = declarative_base()


def load_json(path):
    """Load a json file safely.

    Args:
        path (str): The path to load.

    Returns:
        Any: The data from the json file.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class BaseOrm:
    """Base class to use when creating and populating ORM tables."""

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.engine = create_engine(os.getenv("DATABASE_URL"))

    def drop_all_tables(self):
        """Drop all tables in the database, all at once."""

        metadata = MetaData(schema="public")
        metadata.reflect(bind=self.engine)
        with self.engine.begin() as conn:
            for tbl in reversed(metadata.sorted_tables):
                conn.execute(tbl.delete())
