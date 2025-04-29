"""Maintain and load data for `amendments` table."""

import glob
import json
from database.base import Base, BaseOrm
from sqlalchemy import Column, String, ForeignKey, text, inspect
from sqlalchemy.orm import Session, relationship


class Amendment(Base):
    """ORM class for amendments."""

    __tablename__ = "amendments"

    amendment_id = Column(String, primary_key=True)
    bill_id = Column(String, ForeignKey("bills.bill_id"))
    sponsor_id = Column(String, ForeignKey("legislators.bioguide_id"))
    chamber = Column(String, nullable=False)
    purpose = Column(String)
    congress = Column(String, nullable=False)
    source_filename = Column(String, nullable=False)

    sponsor = relationship("Legislator")
    bill = relationship("Bill")


class AmendmentOrm(BaseOrm):
    """ORM class to interact with the amendments table."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def drop_all_tables(self):
        """Override to restrict dropping tables."""
        raise NotImplementedError("This operation is not allowed in subclasses.")

    def create_table(self):
        """Create the bills table."""
        if not inspect(self.engine).has_table(Amendment.__tablename__):
            Amendment.__table__.create(self.engine)

    def drop_table(self):
        """Drop the bills table."""
        if inspect(self.engine).has_table(Amendment.__tablename__):
            Amendment.__table__.drop(self.engine)

    def populate(self):
        congress_nums = [
            d.replace("./", "")
            for d in glob.glob("./[0-9]*", root_dir=self.data_dir, recursive=False)
            if d.replace("./", "", 1).isdigit()
        ]

        with Session(self.engine) as session:
            session.execute(text(f"DELETE from {Amendment.__tablename__}"))
            session.commit()

            for congress_num in congress_nums:
                amendment_pathspec = f"{self.data_dir}/{congress_num}/amendments"
                datafiles = glob.glob(
                    "**/*.json", root_dir=amendment_pathspec, recursive=True
                )
                for datafile in datafiles:
                    pathspec = f"{amendment_pathspec}/{datafile}"
                    with open(pathspec, "r", encoding="utf-8") as f:
                        data = json.loads(f.read())

                    amendment = Amendment(
                        amendment_id=data.get("amendment_id"),
                        bill_id=data.get("amends_bill").get("bill_id"),
                        sponsor_id=data.get("sponsor").get("bioguide_id"),
                        chamber=data.get("chamber"),
                        purpose=data.get("purpose"),
                        congress=data.get("congress"),
                        source_filename=pathspec.replace(amendment_pathspec, ""),
                    )

                    session.add(amendment)

            session.commit()
