"""Define base methods for working with the database."""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base


# Declarative base
Base = declarative_base()


class BaseOrm:
    """Base class to use when creating and populating ORM tables."""

    def __init__(self, data_dir):
        self.data_dir = data_dir

        db_path = os.path.join(data_dir, "congressgov.db")
        self.engine = create_engine(f"sqlite:///{db_path}")
