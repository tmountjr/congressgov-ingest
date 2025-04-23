"""Maintain and load data for `bills` table."""

import glob
import json
from dateutil import parser
from database.base import Base, BaseOrm
from sqlalchemy import Column, String, ForeignKey, DateTime, text, inspect
from sqlalchemy.orm import Session, relationship


class Bill(Base):
    """
    ORM class for individual bills.
    """

    __tablename__ = "bills"

    bill_id = Column(String, primary_key=True)
    bill_type = Column(String, nullable=False)
    bill_number = Column(String, nullable=False)
    title = Column(String, nullable=False)
    short_title = Column(String)
    sponsor_id = Column(String, ForeignKey("legislators.bioguide_id"))
    status = Column(String, nullable=False)
    status_at = Column(DateTime, nullable=False)
    congress = Column(String, nullable=False)
    source_filename = Column(String, nullable=False)

    # Relationship to Legislators
    sponsor = relationship("Legislator")


class BillOrm(BaseOrm):
    """ORM class to interact with the bills table."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def create_table(self):
        """Create the bills table."""
        if not inspect(self.engine).has_table(Bill.__tablename__):
            Bill.__table__.create(self.engine)


    def drop_table(self):
        """Drop the bills table."""
        if inspect(self.engine).has_table(Bill.__tablename__):
            Bill.__table__.drop(self.engine)


    def populate(self):
        """Ingest bill information."""

        congress_nums = [
            d.replace("./", "")
            for d in glob.glob("./[0-9]*", root_dir=self.data_dir, recursive=False)
            if d.replace("./", "", 1).isdigit()
        ]

        with Session(self.engine) as session:
            session.execute(text(f"DELETE from {Bill.__tablename__}"))
            session.commit()

            for congress_num in congress_nums:
                bills_pathspec = f"{self.data_dir}/{congress_num}/bills"
                datafiles = glob.glob("**/*.json", root_dir=bills_pathspec, recursive=True)
                for datafile in datafiles:
                    pathspec = f"{bills_pathspec}/{datafile}"
                    with open(pathspec, "r", encoding="utf-8") as f:
                        data = json.loads(f.read())

                    bill = Bill(
                        source_filename=pathspec.replace('../congress/', ''),
                        bill_id=data.get("bill_id"),
                        bill_type=data.get("bill_type"),
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
