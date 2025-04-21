"""Maintain and load data for `legislators` table."""

import os
import json
from database.base import Base, BaseOrm
from sqlalchemy import Column, String, text, inspect
from sqlalchemy.orm import Session


class Legislator(Base):
    """
    ORM class for individual legislators.
    """

    __tablename__ = "legislators"

    bioguide_id = Column(String)
    lis_id = Column(String)
    id = Column(String, primary_key=True)
    name = Column(String)
    term_type = Column(String)
    state = Column(String)
    district = Column(String)
    party = Column(String)
    url = Column(String)
    address = Column(String)
    phone = Column(String)


class LegislatorOrm(BaseOrm):
    """ORM class for legislators."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def create_table(self):
        """Create the legislators table."""
        if not inspect(self.engine).has_table(Legislator.__tablename__):
            Legislator.__table__.create(self.engine)

    def drop_table(self):
        """Drop the legislators table."""
        if inspect(self.engine).has_table(Legislator.__tablename__):
            Legislator.__table__.drop(self.engine)

    def populate(self):
        """Ingest legislators information."""

        pathspec = os.path.join(self.data_dir, "legislators-current.json")
        with open(pathspec, "r", encoding="utf-8") as f:
            data = json.loads(f.read())

        with Session(self.engine) as session:
            # Truncate the table first
            session.execute(text(f"DELETE FROM {Legislator.__tablename__}"))
            session.commit()

            for record in data:
                term = record.get("terms")[-1]
                party_name = term.get("party")
                party_shortname = party_name[0]
                legislator_id = record.get("id")

                legislator = Legislator(
                    bioguide_id=record.get("id").get("bioguide"),
                    lis_id=record.get("id").get("lis"),
                    id=(
                        legislator_id.get("lis")
                        if term.get("type") == "sen"
                        else legislator_id.get("bioguide")
                    ),
                    name=record.get("name").get("official_full"),
                    term_type=term.get("type"),
                    state=term.get("state"),
                    district=term.get("district", "N/A"),
                    party=party_shortname,
                    url=term.get("url"),
                    address=term.get("address"),
                    phone=term.get("phone"),
                )

                # Add to the session
                session.add(legislator)

            # Commit changes to the database
            session.commit()
