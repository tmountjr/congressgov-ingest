"""Maintain and load data for `congress` table."""

import os
import glob
import json
import string
import requests
from database.base import Base, BaseOrm
from sqlalchemy import Column, DateTime, Integer, String, inspect, text, select
from sqlalchemy.sql import functions
from sqlalchemy.orm import Session

API_KEY = os.environ.get("CONGRESS_API_KEY")
API_URL = string.Template(
    f"https://api.congress.gov/v3/congress/$num?format=json&api_key={API_KEY}"
)


class Congress(Base):
    """ORM class for the congress information."""

    __tablename__ = "congress"

    congress = Column(String, primary_key=True)
    chamber = Column(String, primary_key=True)
    session = Column(Integer, primary_key=True)
    party = Column(String, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=True)


class CongressOrm(BaseOrm):
    """ORM class for the Congress table."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def drop_all_tables(self):
        """Override to restrict dropping tables."""
        raise NotImplementedError("This operation is not allowed in subclasses.")

    def create_table(self):
        """Create the congress table."""
        if not inspect(self.engine).has_table(Congress.__tablename__):
            Congress.__table__.create(self.engine)

    def drop_table(self):
        """Drop the congress table."""
        if inspect(self.engine).has_table(Congress.__tablename__):
            Congress.__table__.drop(self.engine)

    def populate(self):
        """Ingest congress information."""

        # Rather than look at command line arguments, rely on what congresses have
        # already been imported, and get information for those sessions instead.

        congress_nums = [
            d.replace("./", "")
            for d in glob.glob("./[0-9]*", root_dir=self.data_dir, recursive=False)
            if d.replace("./", "", 1).isdigit()
        ]

        with Session(self.engine) as session:
            for congress_num in congress_nums:
                print(f"Fetching information for Congress # {congress_num}...")
                url = API_URL.substitute(num=congress_num)
                congress_data = requests.get(url, timeout=10)
                if congress_data.status_code == 200:

                    data = json.loads(congress_data.text)
                    congress_data = data.get("congress")

                    sessions = congress_data.get("sessions", [])
                    if len(sessions) > 0:
                        # Only clear previous information if the api call was a success.
                        session.execute(
                            text(
                                f"DELETE FROM {Congress.__tablename__} WHERE congress = :congress"
                            ),
                            params={"congress": congress_num},
                        )
                        session.commit()

                        for s in congress_data.get("sessions", []):
                            chamber_raw = s.get("chamber")
                            chamber = "s" if chamber_raw.lower() == "senate" else "h"

                            this_session = s.get("number")
                            party = s.get("type")
                            start_date = s.get("startDate")
                            end_date = s.get("endDate", None)

                            this_congress = Congress(
                                congress=congress_num,
                                chamber=chamber,
                                session=this_session,
                                party=party,
                                start_date=start_date,
                                end_date=end_date,
                            )

                            # Add to db session for each session of congress found
                            session.add(this_congress)
                    else:
                        print("No data found for Congress {congress_num}. Skipping.")
                else:
                    print(
                        f"Failed to fetch data for Congress {congress_num}. Status code: {congress_data.status_code}"
                    )
                    print(f"URL used: {url}")

            # Commit everything once we have all the sessions needed.
            session.commit()

    def get_count(self, congress_num):
        """
        Get count of records in Congress table matching a specific congress_num.
        """
        with Session(self.engine) as session:
            statement = (
                select(functions.count())
                    .select_from(Congress)
                    .where(Congress.congress == str(congress_num))
            )
            return session.execute(statement).scalar()
