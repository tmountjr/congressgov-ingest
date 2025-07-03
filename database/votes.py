"""Maintain and load data for `vote_meta` and `votes` tables."""

import os
import re
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
from database.base import Base, BaseOrm, load_json
from database.amendments import AmendmentOrm
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, relationship
from sqlalchemy.sql import functions
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    inspect,
    select,
)


class VoteMeta(Base):
    """Vote metadata table."""

    __tablename__ = "vote_meta"

    vote_number = Column(Integer, nullable=False)
    vote_id = Column(String, primary_key=True)
    bill_id = Column(String, nullable=True)
    chamber = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    result = Column(String, nullable=False)
    category = Column(String, nullable=False)
    nomination_title = Column(String)
    amendment_id = Column(String, ForeignKey("amendments.amendment_id"), nullable=True)
    source_filename = Column(String, nullable=False)

    # Relationships
    amendment = relationship("Amendment")


class Vote(Base):
    """
    ORM class for individual votes.
    """

    __tablename__ = "votes"

    vote_id = Column(String, ForeignKey("vote_meta.vote_id"), primary_key=True)
    legislator_id = Column(String, ForeignKey("legislators.id"), primary_key=True)
    position = Column(String, nullable=False)
    original_position = Column(String, nullable=False)

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
        self.amendment_orm = AmendmentOrm(data_dir)

    def drop_all_tables(self):
        """Override to restrict dropping tables."""
        raise NotImplementedError("This operation is not allowed in subclasses.")

    def create_table(self):
        """Create the vote_meta and votes tables in the database."""
        if not inspect(self.engine).has_table(VoteMeta.__tablename__):
            VoteMeta.__table__.create(self.engine)

        if not inspect(self.engine).has_table(Vote.__tablename__):
            Vote.__table__.create(self.engine)

    def drop_table(self):
        """Drop the vote_meta and votes tables from the database."""
        if inspect(self.engine).has_table(Vote.__tablename__):
            Vote.__table__.drop(self.engine)

        if inspect(self.engine).has_table(VoteMeta.__tablename__):
            VoteMeta.__table__.drop(self.engine)

    def upsert_vote_batch(self, session: Session, records: list):
        """Upsert a record into the database."""
        stmt = insert(Vote).values(records)

        stmt = stmt.on_conflict_do_update(
            index_elements=["vote_id", "legislator_id"],
            set_={
                "position": stmt.excluded.position,
                "original_position": stmt.excluded.original_position,
            },
        )

        try:
            session.execute(stmt)
        except IntegrityError as e:
            print(f"Failed to upsert batch: {e}")
            session.rollback()

    def upsert_vote_meta(self, session: Session, record_dict: dict):
        """Upsert a record into the database."""
        stmt = insert(VoteMeta).values(**record_dict)
        update_dict = {
            col: stmt.excluded[col] for col in record_dict if col != "vote_id"
        }

        stmt = stmt.on_conflict_do_update(index_elements=["vote_id"], set_=update_dict)

        try:
            session.execute(stmt)
        except IntegrityError as e:
            print(f"Failed to upsert {record_dict.get('vote_id')}: {e}")
            session.rollback()

    def _amendment_datafile_exists(self, amendment_id: str):
        """Determine if an amendment id has a corresponding data file."""
        pattern = re.compile(r"^(([sh]amdt)\d+)-(\d+$)")
        match = pattern.match(amendment_id)
        if not match:
            return False

        type_extended, type_basic, congress_num = match.group(1, 2, 3)
        return os.path.exists(
            os.path.join(
                self.data_dir,
                congress_num,
                "amendments",
                type_basic,
                type_extended,
                "data.json",
            )
        )

    def populate(self):
        """
        Ingest votes and metadata.

        Because votes and vote metadata are so tightly coupled, always drop and reload
        them at the same time.
        """

        vote_files = [
            str(path)
            for path in Path(self.data_dir).glob("*/votes/*/*/data.json")
            if path.is_file() and path.parts[-5].isdigit()
        ]

        with ThreadPoolExecutor() as executor:
            vote_data = [
                {"vote_file": str(path), "data": data}
                for path, data in zip(vote_files, executor.map(load_json, vote_files))
            ]

        with Session(self.engine) as session:
            # Rather than do the ORM bit piece by piece, store them and batch
            # upsert them all at once.
            vote_meta_entries = []
            vote_entries = []

            for entry in vote_data:
                vote_file: str = entry["vote_file"]
                data: dict = entry["data"]

                # Get vote metadata first
                bill_info = data.get("bill")
                if bill_info:
                    b_type = bill_info.get("type")
                    b_number = bill_info.get("number")
                    b_congress = bill_info.get("congress")
                    bill_id = f"{b_type}{b_number}-{b_congress}"
                else:
                    bill_id = None
                chamber = data.get("chamber")
                is_nomination = "nomination" in data
                nomination_title = (
                    None if not is_nomination else data.get("nomination").get("title")
                )
                amendment = data.get("amendment")
                amendment_id = None  # and we'll override it if we can.

                if amendment:
                    amendment_number = amendment.get("number")
                    amendment_type = amendment.get("type")
                    congress = str(data.get("congress"))
                    # Senate amendments are straightforward.
                    # House ones not so much. When the type = "h-bill":
                    #   - the number is the amendment number _to that
                    #     bill_, not the amendment id number
                    #   - you have to go back to the bill itself, to the
                    #     "amendments" key, reverse the array so the first
                    #     amendment is index 0, then find the nth
                    #     amendment, then get the amendment_id from there

                    if amendment_type in ("s", "h"):
                        amendment_id = (
                            f"{amendment_type}amdt{amendment_number}-{congress}"
                        )
                    elif amendment_type == "h-bill":
                        obd = data.get("bill")
                        obd_type = obd.get("type")
                        bill_number = f"{obd_type}{obd.get("number")}"  # eg. hr1048
                        bill_pathspec = os.path.join(
                            self.data_dir,
                            str(congress),
                            "bills",
                            obd_type,
                            str(bill_number),
                            "data.json",
                        )

                        if os.path.exists(bill_pathspec):
                            with open(bill_pathspec, "r", encoding="utf-8") as bill_f:
                                bill_data = json.loads(bill_f.read())
                            bill_amendments = bill_data.get("amendments", [])
                            if bill_amendments:
                                amendment_id = bill_amendments[
                                    -1 * int(amendment_number)
                                ].get("amendment_id")

                    # If we've gotten to this point and still don't have an
                    # # amendment_id, just skip the amendment altogether.
                    if (
                        amendment_id is not None
                        and not self._amendment_datafile_exists(amendment_id)
                    ):
                        # P000000 is a placeholder in the legislator table.
                        print(f"Creating placeholder amendment for {amendment_id}...")
                        self.amendment_orm.create_placeholder(
                            amendment_id=amendment_id,
                            bill_id=bill_id,
                            sponsor_id="P000000",
                            congress=congress,
                            session=session,
                        )

                vote_meta_entry = {
                    "vote_number": data.get("number"),
                    "vote_id": data.get("vote_id"),
                    "bill_id": bill_id,
                    "chamber": chamber,
                    "date": parser.parse(data.get("date")),
                    "result": data.get("result_text"),
                    "category": data.get("category").strip(),
                    "nomination_title": nomination_title,
                    "amendment_id": amendment_id,
                    "source_filename": vote_file,
                }
                vote_meta_entries.append(vote_meta_entry)
                # Add to session
                # self.upsert_vote_meta(session, vote_meta)
                # session.flush()

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
                                legislator_lid if chamber == "s" else legislator_bid
                            )
                            normalized_response = NORMALIZE_RESPONSES.get(
                                response, response
                            )

                            vote = {
                                "vote_id": vote_id,
                                "legislator_id": legislator_id,
                                "position": normalized_response,
                                # Store the original response for reference
                                "original_position": response,
                            }

                            vote_entries.append(vote)
                            # Add to sesssion
                            # self.upsert_vote(session, vote)
                            # session.flush()

            # Commit changes
            # Push the vote_meta entries and commit.
            for vote_meta_entry in vote_meta_entries:
                self.upsert_vote_meta(session, vote_meta_entry)
            session.commit()

            # The number of votes can easily be in the 100s of thousands.
            # Batch commit 1000 at a time.
            batch_size = 1000
            for i in range(0, len(vote_entries), batch_size):
                batch = vote_entries[i : i + batch_size]
                self.upsert_vote_batch(session, batch)
                session.commit()

    def get_count(self, congress_num: int | None = None):
        """Count the number of vote metadata entries."""
        with Session(self.engine) as session:
            statement = select(functions.count(1)).select_from(VoteMeta)
            if congress_num is not None:
                statement = statement.where(
                    VoteMeta.vote_id.like(f"%-{congress_num}.%")
                )
            return session.execute(statement).scalar()
