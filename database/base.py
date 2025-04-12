from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, declarative_base


# Declarative base
Base = declarative_base()

# Database engine
engine = create_engine("sqlite:///congressgov.db")

# Create some views
with Session(engine) as session:
    # Create a view that shows only the last vote on each vote ID.
    session.execute(
        text(
            """create view if not exists latest_vote_ids as
  select vm.*
  from vote_meta vm
  join (
    select bill_id, max(date) as latest_date
    from vote_meta
    group by bill_id
  ) latest_votes
  on
    vm.bill_id = latest_votes.bill_id
    and vm.date = latest_votes.latest_date;"""
        )
    )

    # Create a view that adds sponsor name and party to the vote_meta table.
    session.execute(
        text(
            """create view enriched_vote_meta as
  select vote_meta.*, legislators.name as sponsor_name, legislators.party as sponsor_party
  from vote_meta
  left join bills on vote_meta.bill_id = bills.bill_id
  left join legislators on bills.sponsor_id = legislators.bioguide_id;"""
        )
    )
