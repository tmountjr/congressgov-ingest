"""Define base methods for working with the database."""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base


load_dotenv()

# Declarative base
Base = declarative_base()


class BaseOrm:
    """Base class to use when creating and populating ORM tables."""

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.engine = create_engine(os.getenv("DATABASE_URL"))

    def drop_all_tables(self):
        """Drop all tables in the database, all at once."""

        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        with self.engine.begin() as conn:
            for tbl in reversed(metadata.sorted_tables):
                conn.execute(tbl.delete())
