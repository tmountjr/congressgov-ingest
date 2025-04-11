"""
Main script to ingest data
"""

from database.legislators import (
  create_table as create_legislators_table,
  populate as populate_legislators_table
)
from database.bills import (
  create_table as create_bills_table,
  populate as populate_bills_table
)
from database.votes import (
  create_table as create_votes_table,
  populate as populate_votes_table
)

if __name__ == "__main__":
    print("Importing legislators...")
    create_legislators_table()
    populate_legislators_table()

    print("Importing bills...")
    create_bills_table()
    populate_bills_table()

    print("Importing votes and vote metadata...")
    create_votes_table()
    populate_votes_table()
