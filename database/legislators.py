"""Maintain and load data for `legislators` table."""

import os
import json
from string import Template
import requests
from database.base import Base, BaseOrm
from sqlalchemy import Column, String, text, inspect
from sqlalchemy.orm import Session


class Legislator(Base):
    """
    ORM class for individual legislators.
    """

    __tablename__ = "legislators"

    bioguide_id = Column(String, unique=True, nullable=False)
    lis_id = Column(String)
    id = Column(String, primary_key=True, nullable=False)
    name = Column(String, nullable=False)
    term_type = Column(String, nullable=False)
    state = Column(String, nullable=False)
    district = Column(String, nullable=False)
    party = Column(String, nullable=False)
    url = Column(String)
    address = Column(String)
    phone = Column(String)
    caucus = Column(String)


class LegislatorOrm(BaseOrm):
    """ORM class for legislators."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def drop_all_tables(self):
        """Override to restrict dropping tables."""
        raise NotImplementedError("This operation is not allowed in subclasses.")

    def create_table(self):
        """Create the legislators table."""
        if not inspect(self.engine).has_table(Legislator.__tablename__):
            Legislator.__table__.create(self.engine)

    def drop_table(self):
        """Drop the legislators table."""
        if inspect(self.engine).has_table(Legislator.__tablename__):
            Legislator.__table__.drop(self.engine)

    def fetch_list(self):
        """Fetch the legislator list from the online JSON file."""

        merged_legislators = []
        base_url = Template(
            "https://unitedstates.github.io/congress-legislators/legislators-$type.json"
        )

        for url in [base_url.substitute(type=t) for t in ["current", "historical"]]:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            json_data = json.loads(response.text)
            merged_legislators.extend(json_data)

        with open(
            os.path.join(self.data_dir, "legislators.json"), "w", encoding="utf-8"
        ) as f:

            json.dump(merged_legislators, f, indent=2)

    def populate(self):
        """Ingest legislators information."""

        pathspec = os.path.join(self.data_dir, "legislators.json")
        with open(pathspec, "r", encoding="utf-8") as f:
            data = json.loads(f.read())

        with Session(self.engine) as session:
            # Truncate the table first
            session.execute(text(f"DELETE FROM {Legislator.__tablename__}"))
            session.commit()

            for record in data:
                # We should arbitrarily cut off anyone whose final term in office started pre-2010.
                final_term = record.get("terms")[-1]
                final_term_end = final_term.get("end", "2000-01-01")
                (year, month, day) = final_term_end.split("-")
                if int(year) < 2010:
                    continue

                try:
                    term = record.get("terms")[-1]
                    party_name = term.get("party", "None")
                    party_shortname = (
                        party_name[0]
                        if party_name[0] == "D"
                        or party_name[0] == "R"
                        or party_name[0] == "I"
                        else "-"
                    )
                    caucus = term.get("caucus", party_shortname)[0]
                    legislator_id = record.get("id")
                    name_record = record.get("name")
                    name = name_record.get("official_full")
                    if not name:
                        name = f"{name_record.get("first")} {name_record.get("last")}"

                    legislator = Legislator(
                        bioguide_id=record.get("id").get("bioguide"),
                        lis_id=record.get("id").get("lis"),
                        id=(
                            legislator_id.get("lis")
                            if term.get("type") == "sen"
                            else legislator_id.get("bioguide")
                        ),
                        name=name,
                        term_type=term.get("type"),
                        state=term.get("state"),
                        district=term.get("district", "N/A"),
                        party=party_shortname,
                        url=term.get("url"),
                        address=term.get("address"),
                        phone=term.get("phone"),
                        caucus=caucus,
                    )
                except TypeError as e:
                    print(f"Error processing record: {record}")
                    print(e)
                    continue

                # Add to the session
                session.add(legislator)

            # Commit changes to the database
            session.commit()
