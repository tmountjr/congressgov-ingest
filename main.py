"""
Main script to ingest data
"""

from database.legislators import (
  drop_table as drop_legislators_table,
  create_table as create_legislators_table,
  populate as populate_legislators_table
)
from database.bills import (
  drop_table as drop_bills_table,
  create_table as create_bills_table,
  populate as populate_bills_table
)
from database.votes import (
  drop_table as drop_votes_table,
  create_table as create_votes_table,
  populate as populate_votes_table
)

if __name__ == "__main__":
    print("Importing legislators...")
    drop_legislators_table()
    create_legislators_table()
    populate_legislators_table()

    print("Importing bills...")
    drop_bills_table()
    create_bills_table()
    populate_bills_table()

    print("Importing votes and vote metadata...")
    drop_votes_table()
    create_votes_table()
    populate_votes_table()
