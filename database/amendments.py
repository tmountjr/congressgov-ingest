"""Maintain and load data for `amendments` table."""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from database.base import Base, BaseOrm, load_json
from sqlalchemy import Column, String, ForeignKey, inspect
from sqlalchemy.orm import Session, relationship
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError


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

    def upsert(self, session: Session, record_dict: dict):
        """Upsert a record into the database."""
        stmt = insert(Amendment).values(**record_dict)
        update_dict = {
            col: stmt.excluded[col] for col in record_dict if col != "amendment_id"
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=["amendment_id"], set_=update_dict
        )

        try:
            session.execute(stmt)
        except IntegrityError as e:
            print(f"Failed to upsert {record_dict.get('amendment_id')}: {e}")
            session.rollback()

    def populate(self):
        """Populate the Amendments table."""

        amendment_files = [
            str(path)
            for path in Path(self.data_dir).glob("*/amendments/*/*/data.json")
            if path.is_file()
        ]

        with ThreadPoolExecutor() as executor:
            amendment_data = [
                {"filename": str(path), "data": data}
                for path, data in zip(
                    amendment_files, executor.map(load_json, amendment_files)
                )
            ]

        with Session(self.engine) as session:
            for entry in amendment_data:
                filename = entry["filename"]
                data = entry["data"]

                amendment = {
                    "amendment_id": data.get("amendment_id"),
                    "bill_id": data.get("amends_bill").get("bill_id"),
                    "sponsor_id": data.get("sponsor").get("bioguide_id"),
                    "chamber": data.get("chamber"),
                    "purpose": data.get("purpose"),
                    "congress": data.get("congress"),
                    "source_filename": filename,
                }

                self.upsert(session, amendment)

            session.commit()

    def create_placeholder(self, amendment_id, bill_id, sponsor_id, congress, session):
        """Create a placeholder amendment so that a vote can be stored."""

        amendment = {
            "amendment_id": amendment_id,
            "bill_id": bill_id,
            "sponsor_id": sponsor_id,
            "chamber": amendment_id[0],
            "purpose": "[PLACEHOLDER] Amendment datafile not yet downloaded.",
            "congress": congress,
            "source_filename": "na",
        }

        # Don't create a new session, use the injected one instead.
        # Also don't run session.commit() because we may not want to commit at
        # this stage.
        self.upsert(session, amendment)
        session.flush()
