"""Maintain and load data for `bills` table."""

import glob
import json
from dateutil import parser
from database.base import Base, engine
from sqlalchemy.orm import Session, relationship
from sqlalchemy import Column, String, ForeignKey, DateTime, text


class Bill(Base):
    """
    ORM class for individual bills.
    """

    __tablename__ = "bills"

    bill_id = Column(String, primary_key=True)
    bill_number = Column(String)
    title = Column(String)
    short_title = Column(String)
    sponsor_id = Column(String, ForeignKey("legislators.bioguide_id"))
    status = Column(String)
    status_at = Column(DateTime)
    congress = Column(String)
    source_filename = Column(String)

    # Relationship to Legislators
    sponsor = relationship("Legislator")


def create_table():
    """Create the bills table."""
    Bill.__table__.create(engine)


def drop_table():
    """Drop the bills table."""
    Bill.__table__.drop(engine)


def populate():
    """Ingest bill information."""

    root_pathspec = "../congress/data"
    congress_nums = [
        d.replace("./", "")
        for d in glob.glob("./[0-9]*", root_dir=root_pathspec, recursive=False)
        if d.replace("./", "", 1).isdigit()
    ]

    with Session(engine) as session:
        session.execute(text(f"DELETE from {Bill.__tablename__}"))
        session.commit()

        for congress_num in congress_nums:
            bills_pathspec = f"{root_pathspec}/{congress_num}/bills"
            datafiles = glob.glob("**/*.json", root_dir=bills_pathspec, recursive=True)
            for datafile in datafiles:
                pathspec = f"{bills_pathspec}/{datafile}"
                with open(pathspec, "r", encoding="utf-8") as f:
                    data = json.loads(f.read())

                bill = Bill(
                    source_filename=pathspec.replace('../congress/', ''),
                    bill_id=data.get("bill_id"),
                    bill_number=data.get("number"),
                    title=data.get("official_title"),
                    short_title=data.get("short_title"),
                    sponsor_id=data.get("sponsor").get("bioguide_id"),
                    status=data.get("status"),
                    status_at=parser.parse(data.get("status_at")),
                    congress=data.get("congress"),
                )

                # Add to the session
                session.add(bill)

        # Commit changes to the database
        session.commit()
