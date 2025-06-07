"""
Main script to ingest data
"""


if __name__ == "__main__":

    import os
    import argparse
    from dotenv import dotenv_values
    from database.base import BaseOrm
    from database.bills import BillOrm
    from database.votes import VoteOrm
    from database.views import create_views
    from database.congress import CongressOrm
    from database.site_meta import SiteMetaOrm
    from database.amendments import AmendmentOrm
    from database.legislators import LegislatorOrm
    from sanity_check import SanityCheck
    from stdout_logger import StdoutLogger

    logger = StdoutLogger(__name__)

    # Check if we've passed "data_dir=" to the script, and if so, use it in the
    # populate methods.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        default="data",
        help="Directory containing the data (default: 'data')",
    )
    parser.add_argument(
        "--environment",
        default="prod",
        type=str,
        help="The environment to use (default: 'prod')",
    )

    args = parser.parse_args()

    base_env = dotenv_values(".env")
    if args.environment != "prod":
        override_env = dotenv_values(f".env.{args.environment}")
        base_env.update(override_env)
    os.environ.update(base_env)

    logger.info("Dropping all tables in preparation for data load...")
    base_orm = BaseOrm(args.data_dir)
    base_orm.drop_all_tables()

    logger.info("Importing legislators...")
    legis_orm = LegislatorOrm(args.data_dir)
    legis_orm.create_table()
    legis_orm.populate()

    logger.info("Importing bills...")
    bill_orm = BillOrm(args.data_dir)
    bill_orm.create_table()
    bill_orm.populate()

    logger.info("Importing amendments...")
    amend_orm = AmendmentOrm(args.data_dir)
    amend_orm.create_table()
    amend_orm.populate()

    logger.info("Importing votes and vote metadata...")
    vote_orm = VoteOrm(args.data_dir)
    vote_orm.create_table()
    vote_orm.populate()

    logger.info("Importing Congress session metadata...")
    congress_orm = CongressOrm(args.data_dir)
    congress_orm.create_table()
    congress_orm.populate()

    logger.info("Setting up views...")
    # TODO: what's interesting here is that when running for the first time,
    # no views were created.
    create_views(args.data_dir)

    # Update the database with the latest update time
    logger.info("Updating site metadata...")
    site_meta_orm = SiteMetaOrm()
    site_meta_orm.create_table()
    site_meta_orm.set_last_update()

    logger.info("Running sanity checks...")
    checker = SanityCheck(args.data_dir)
    checker.run()

    logger.info("Done!")
