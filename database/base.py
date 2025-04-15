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
            """DROP VIEW IF EXISTS "main"."latest_vote_ids";
CREATE VIEW latest_vote_ids as
	with temp_vm as (
		select
			*,
			case
				when bill_id = 'N/A'
				then substr(nomination_title, 1, 10)
				else bill_id
			end as unique_matching_field
		from vote_meta vm
	)

	select vm.*
	from temp_vm vm
	join (
		select unique_matching_field, max(date) as latest_date
		from temp_vm
		group by unique_matching_field
	) latest_votes
	on vm.unique_matching_field = latest_votes.unique_matching_field and vm.date = latest_votes.latest_date
"""
        )
    )

    # Create a view that adds sponsor name and party to the vote_meta table.
    session.execute(
        text(
            """DROP VIEW IF EXISTS "main"."enriched_vote_meta";
CREATE VIEW enriched_vote_meta as
  select vote_meta.*, legislators.name as sponsor_name, legislators.party as sponsor_party
  from vote_meta
  left join bills on vote_meta.bill_id = bills.bill_id
  left join legislators on bills.sponsor_id = legislators.bioguide_id"""
        )
    )
