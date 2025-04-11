from sqlalchemy import Column, String, text
from sqlalchemy.orm import Session
from database.base import Base, engine
import json


class Legislator(Base):
    __tablename__ = "legislators"

    bioguide_id = Column(String, primary_key=True)
    name = Column(String)
    term_type = Column(String)
    state = Column(String)
    district = Column(String, nullable=True)
    party = Column(String)
    url = Column(String)
    address = Column(String)
    phone = Column(String)


def create_table():
    """Create the legislators table."""
    Legislator.__table__.create(engine)


def drop_table():
    """Drop the legislators table."""
    Legislator.__table__.drop(engine)

def populate():
    """Ingest legislators information."""

    pathspec = "./legislators-current.json"
    with open(pathspec, "r", encoding="utf-8") as f:
        data = json.loads(f.read())

    with Session(engine) as session:
        # Truncate the table first
        session.execute(text(f"DELETE FROM {Legislator.__tablename__}"))
        session.commit()

        for record in data:
            term = record.get("terms")[-1]
            party_name = term.get("party")
            party_shortname = party_name[0]

            legislator = Legislator(
                bioguide_id=record.get("id").get("bioguide"),
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
