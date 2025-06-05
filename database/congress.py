"""Maintain and load data for `congress` table."""

import glob
import json
import os
import requests
from database.base import Base, BaseOrm
from sqlalchemy import Column, DateTime, Integer, String, inspect, text, select
from sqlalchemy.sql import functions
from sqlalchemy.orm import Session


class Congress(Base):
    """ORM class for the congress information."""

    __tablename__ = "congress"

    congress = Column(String, primary_key=True)
    chamber = Column(String, primary_key=True)
    session = Column(Integer, primary_key=True)
    party = Column(String, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=True)


class CongressOrm(BaseOrm):
    """ORM class for the Congress table."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def drop_all_tables(self):
        """Override to restrict dropping tables."""
        raise NotImplementedError("This operation is not allowed in subclasses.")

    def create_table(self):
        """Create the congress table."""
        if not inspect(self.engine).has_table(Congress.__tablename__):
            Congress.__table__.create(self.engine)

    def drop_table(self):
        """Drop the congress table."""
        if inspect(self.engine).has_table(Congress.__tablename__):
            Congress.__table__.drop(self.engine)

    def populate(self):
        """Ingest congress information."""

        metadata_path = os.path.join(self.data_dir, "congress.json")
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.loads(f.read())
            if len(metadata) > 0:
                with Session(self.engine) as session:
                    session.execute(text(f"DELETE FROM {Congress.__tablename__}"))
                    session.commit()

                    for item in metadata:
                        c = Congress(**item)
                        session.add(c)

                    session.commit()
            else:
                print("Congress metadata not found. Skipping.")

    def get_count(self, congress_num):
        """
        Get count of records in Congress table matching a specific congress_num.
        """
        with Session(self.engine) as session:
            statement = (
                select(functions.count())
                .select_from(Congress)
                .where(Congress.congress == str(congress_num))
            )
            return session.execute(statement).scalar()
