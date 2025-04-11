import glob
import json
from dateutil import parser
from database.base import Base, engine
from sqlalchemy.orm import Session, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, text


class VoteMeta(Base):
    """Vote metadata table."""

    __tablename__ = "vote_meta"

    vote_number = Column(Integer)
    vote_id = Column(String, primary_key=True)
    bill_id = Column(String, ForeignKey("bills.bill_id"))
    chamber = Column(String)
    date = Column(DateTime)
    result = Column(String)
    category = Column(String)
    source_filename = Column(String)

    # Relationship to Bills
    bill = relationship("Bill")


def create_table():
    """Create the vote_meta table in the database."""
    VoteMeta.__table__.create(engine)


def drop_table():
    """Drop the vote_meta table from the database."""
    VoteMeta.__table__.drop(engine)


def populate():
    """Ingest vote metadata."""

    root_pathspec = "../congress/data"
    congress_nums = [
        d.replace("./", "")
        for d in glob.glob("./[0-9]*", root_dir=root_pathspec, recursive=False)
        if d.replace("./", "").isdigit()
    ]

    with Session(engine) as session:
        session.execute(text(f"DELETE from {VoteMeta.__tablename__}"))
        session.commit()

        for congress_num in congress_nums:
            years = [
                y.replace("./", "")
                for y in glob.glob(
                    "./[0-9]*", root_dir=f"{root_pathspec}/{congress_num}/votes"
                )
            ]
            for year in years:
                pathspec = f"{root_pathspec}/{congress_num}/votes/{year}"
                vote_files = glob.glob("**/*.json", root_dir=pathspec, recursive=True)
                for vote_file in vote_files:
                    print(f"loading from {pathspec}/{vote_file}")
                    with open(f"{pathspec}/{vote_file}", "r", encoding="utf-8") as f:
                        data = json.load(f)

                    bill_info = data.get("bill")
                    bill_id = "{}{}-{}".format(
                        bill_info.get("type"),
                        bill_info.get("number"),
                        bill_info.get("congress"),
                    ) if bill_info else "N/A"

                    vote_meta = VoteMeta(
                        vote_number=data.get("number"),
                        vote_id=data.get("vote_id"),
                        bill_id=bill_id,
                        chamber=data.get("chamber"),
                        date=parser.parse(data.get("date")),
                        result=data.get("result_text"),
                        category=data.get("category"),
                        source_filename=vote_file,
                    )

                    # Add to session
                    session.add(vote_meta)

        # Commit changes
        session.commit()
