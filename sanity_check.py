"""Sanity Checker main file."""

import argparse
from shared_meta import count_votes, downloaded_sessions
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

    def __init__(self, data_dir, congress_nums: list[str] = []):
        self.data_dir = data_dir
        self.congress_nums = (
            congress_nums if len(congress_nums) > 0 else downloaded_sessions(data_dir)
        )
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
        vote_orm = VoteOrm()

        for congress_num in self.congress_nums:
            expected_vote_count = count_votes(congress_num, self.data_dir)
            actual_vote_count = vote_orm.get_count(congress_num)
            pass_fail = actual_vote_count == expected_vote_count
            print(f"[{congress_num}] Expected vote count: {expected_vote_count}")
            print(f"[{congress_num}] Actual vote count: {actual_vote_count}")
            print(
                f"[{congress_num}] Passes sanity check: {self.color_yes if pass_fail else self.color_no}"
            )
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

        for congress_num in self.congress_nums:
            actual_congress_count = congress_orm.get_count(congress_num)
            pass_fail = actual_congress_count in (2, 4)
            print(f"[{congress_num}] Checking Congress {congress_num} metadata...")
            print(
                f"[{congress_num}] Passes sanity check: {self.color_yes if pass_fail else self.color_no}"
            )
            print("")

    def run(self):
        """Run all sanity checks."""
        print("")
        self._run_legislator_sanity_check()
        self._run_vote_sanity_check()
        self._run_congress_sanity_check()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        default="data",
        help="Directory containing the data (default: 'data')",
        required=True,
    )
    parser.add_argument(
        "--congress",
        help="Congress number to check; separate multiple numbers with a comma",
    )
    args = parser.parse_args()

    congress_args = (
        [int(x) for x in args.congress.split(",") if x.isdigit()]
        if args.congress
        else []
    )

    sanity_check = SanityCheck(data_dir=args.data_dir, congress_nums=congress_args)
    sanity_check.run()
