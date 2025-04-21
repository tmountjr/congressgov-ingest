"""
Main script to ingest data
"""

import argparse
from database.bills import BillOrm
from database.votes import VoteOrm
from database.views import create_views
from database.legislators import LegislatorOrm


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
    legis_orm = LegislatorOrm(args.data_dir)
    legis_orm.fetch_list()
    legis_orm.create_table()
    legis_orm.populate()

    print("Importing bills...")
    bill_orm = BillOrm(args.data_dir)
    bill_orm.create_table()
    bill_orm.populate()

    print("Importing votes and vote metadata...")
    vote_orm = VoteOrm(args.data_dir)
    vote_orm.create_table()
    vote_orm.populate()

    print("Setting up views...")
    create_views(args.data_dir)

    print("Done!")
