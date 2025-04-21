"""Maintain and load data for `vote_meta` and `votes` tables."""

import glob
import json
from dateutil import parser
from database.base import Base, BaseOrm
from sqlalchemy.orm import Session, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, inspect, text


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
    nomination_title = Column(String)
    source_filename = Column(String)

    # Relationship to Bills
    bill = relationship("Bill")


class Vote(Base):
    """
    ORM class for individual votes.
    """

    __tablename__ = "votes"

    vote_id = Column(String, ForeignKey("vote_meta.vote_id"), primary_key=True)
    legislator_id = Column(String, ForeignKey("legislators.id"), primary_key=True)
    position = Column(String)
    original_position = Column(String)

    # Relationships
    legislator = relationship("Legislator")
    vote_meta = relationship("VoteMeta")


# These are the canonical responses to be stored.
KNOWN_RESPONSES = ["Nay", "Not Voting", "Present", "Yea"]

# For non-canonical repsonses, map them to a canonical response.
NORMALIZE_RESPONSES = {
    "Aye": "Yea",
    "No": "Nay",
    # For the speaker race, a vote for Emmer was basically a throwaway vote;
    # a vote for Johnson (R) was a "Yea" vote, and a vote for Jeffries (D) was a
    # "Nay" vote.
    "Emmer": "Present",
    "Johnson (LA)": "Yea",
    "Jeffries": "Nay",
}


class VoteOrm(BaseOrm):
    """Class for interacting with the ORM and reusing a single engine definition."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def create_table(self):
        """Create the vote_meta and votes tables in the database."""
        if not inspect(self.engine).has_table(Vote.__tablename__):
            Vote.__table__.create(self.engine)
        
        if not inspect(self.engine).has_table(VoteMeta.__tablename__):
            VoteMeta.__table__.create(self.engine)

    def drop_table(self):
        """Drop the vote_meta and votes tables from the database."""
        if inspect(self.engine).has_table(Vote.__tablename__):
            Vote.__table__.drop(self.engine)

        if inspect(self.engine).has_table(VoteMeta.__tablename__):
            VoteMeta.__table__.drop(self.engine)

    def populate(self):
        """
        Ingest votes and metadata.

        Because votes and vote metadata are so tightly coupled, always drop and reload
        them at the same time.
        """

        congress_nums = [
            d.replace("./", "")
            for d in glob.glob("./[0-9]*", root_dir=self.data_dir, recursive=False)
            if d.replace("./", "").isdigit()
        ]

        with Session(self.engine) as session:
            session.execute(text(f"DELETE from {Vote.__tablename__}"))
            session.execute(text(f"DELETE from {VoteMeta.__tablename__}"))
            session.commit()

            for congress_num in congress_nums:
                years = [
                    y.replace("./", "")
                    for y in glob.glob(
                        "./[0-9]*", root_dir=f"{self.data_dir}/{congress_num}/votes"
                    )
                ]
                for year in years:
                    pathspec = f"{self.data_dir}/{congress_num}/votes/{year}"
                    vote_files = glob.glob(
                        "**/*.json", root_dir=pathspec, recursive=True
                    )
                    for vote_file in vote_files:
                        with open(
                            f"{pathspec}/{vote_file}", "r", encoding="utf-8"
                        ) as f:
                            data = json.load(f)

                        # Get vote metadata first
                        bill_info = data.get("bill")
                        if bill_info:
                            b_type = bill_info.get("type")
                            b_number = bill_info.get("number")
                            b_congress = bill_info.get("congress")
                            bill_id = f"{b_type}{b_number}-{b_congress}"
                        else:
                            bill_id = "N/A"
                        chamber = data.get("chamber")
                        is_nomination = "nomination" in data
                        nomination_title = (
                            None
                            if not is_nomination
                            else data.get("nomination").get("title")
                        )

                        vote_meta = VoteMeta(
                            vote_number=data.get("number"),
                            vote_id=data.get("vote_id"),
                            bill_id=bill_id,
                            chamber=chamber,
                            date=parser.parse(data.get("date")),
                            result=data.get("result_text"),
                            category=data.get("category").strip(),
                            nomination_title=nomination_title,
                            source_filename=vote_file,
                        )
                        # Add to session
                        session.add(vote_meta)

                        # Load specific votes next
                        votes_data = data.get("votes")
                        for response, people in votes_data.items():
                            vote_id = data.get("vote_id")

                            for person in people:
                                if isinstance(person, dict):
                                    # Determine if the ID present is a lid or a bid
                                    legislator_lid = (
                                        person.get("id") if chamber == "s" else None
                                    )
                                    legislator_bid = (
                                        person.get("id") if chamber == "h" else None
                                    )
                                    legislator_id = (
                                        legislator_lid
                                        if chamber == "s"
                                        else legislator_bid
                                    )
                                    normalized_response = NORMALIZE_RESPONSES.get(response, response)

                                    vote = Vote(
                                        vote_id=vote_id,
                                        legislator_id=legislator_id,
                                        position=normalized_response,
                                        # Store the original response for reference
                                        original_position=response,
                                    )

                                    # Add to sesssion
                                    session.add(vote)

            # Commit changes
            session.commit()
