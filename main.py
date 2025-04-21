"""
Main script to ingest data
"""

import argparse
from database.legislators import (
    create_table as create_legislators_table,
    populate as populate_legislators_table,
)
from database.bills import (
    create_table as create_bills_table,
    populate as populate_bills_table,
)
from database.votes import (
    create_table as create_votes_table,
    populate as populate_votes_table,
)


if __name__ == "__main__":
    # Check if we've passed "data_dir=" to the script, and if so, use it in the
    # populate methods.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        default="data",
        help="Directory containing the data (default: 'data')",
    )

    args = parser.parse_args()

    print("Importing legislators...")
    create_legislators_table()
    populate_legislators_table(args.data_dir)

    print("Importing bills...")
    create_bills_table()
    populate_bills_table(args.data_dir)

    print("Importing votes and vote metadata...")
    create_votes_table()
    populate_votes_table(args.data_dir)
