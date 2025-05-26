"""Sanity Checker main file."""

import argparse
from shared_meta import count_votes
from database.votes import VoteOrm
from database.congress import CongressOrm
from database.legislators import LegislatorOrm

COLORS = {
    "HEADER": "\033[95m",
    "OKBLUE": "\033[94m",
    "OKCYAN": "\033[96m",
    "OKGREEN": "\033[92m",
    "WARNING": "\033[93m",
    "FAIL": "\033[91m",
    "ENDC": "\033[0m",
    "BOLD": "\033[1m",
    "UNDERLINE": "\033[4m",
}


class SanityCheck:
    """Class to run all sanity checks."""

    def __init__(self, congress_num, data_dir):
        self.congress_num = congress_num
        self.data_dir = data_dir
        self.color_yes = (
            f"{COLORS.get("OKGREEN") + COLORS.get("BOLD")}yes{COLORS.get("ENDC")}"
        )
        self.color_no = (
            f"{COLORS.get("FAIL") + COLORS.get("BOLD")}no{COLORS.get("ENDC")}"
        )

    def _run_vote_sanity_check(self):
        """
        Sanity-check votes.
        There should be as many vote entries in the database as there are vote
        files in the data directory.
        """
        expected_vote_count = count_votes(self.congress_num, self.data_dir)
        vote_orm = VoteOrm()
        actual_vote_count = vote_orm.get_count()
        pass_fail = actual_vote_count == expected_vote_count

        print(f"Expected vote count: {expected_vote_count}")
        print(f"Actual vote count: {actual_vote_count}")
        print(f"Passes sanity check: {self.color_yes if pass_fail else self.color_no}")
        print("")

    def _run_legislator_sanity_check(self):
        """
        Sanity-check legislators.
        There should be at least 535 legislators in the database.
        """
        legislator_orm = LegislatorOrm()
        actual_legislator_count = legislator_orm.get_count()
        pass_fail = actual_legislator_count >= 535

        print(f"Actual legislator count: {actual_legislator_count}")
        print(f"Passes sanity check: {self.color_yes if pass_fail else self.color_no}")
        print("")

    def _run_congress_sanity_check(self):
        """
        Sanity-check Congress api data.
        There should be either 2 or 4 sessions of congress for the given
        congress_num.
        """
        congress_orm = CongressOrm()
        actual_congress_count = congress_orm.get_count(119)
        pass_fail = actual_congress_count in (2, 4)
        print("Checking Congress 119...")
        print(f"Passes sanity check: {self.color_yes if pass_fail else self.color_no}")
        print("")

    def run(self):
        """Run all sanity checks."""
        self._run_vote_sanity_check()
        self._run_legislator_sanity_check()
        self._run_congress_sanity_check()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        default="data",
        help="Directory containing the data (default: 'data')",
    )
    parser.add_argument(
        "--congress",
        default=119,
        help="Congress number to check (default: 119)",
    )
    args = parser.parse_args()

    sanity_check = SanityCheck(args.congress, args.data_dir)
    sanity_check.run()
