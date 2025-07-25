"""Sanity Checker main file."""

import argparse
from shared_meta import count_votes, downloaded_sessions
from stdout_logger import StdoutLogger
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
        color_yes = (
            f"{COLORS.get("OKGREEN") + COLORS.get("BOLD")}yes{COLORS.get("ENDC")}"
        )
        color_no = f"{COLORS.get("FAIL") + COLORS.get("BOLD")}no{COLORS.get("ENDC")}"
        self.logger = StdoutLogger(__name__)

        self.passes_sanity_check_message = f"[%s] Passes sanity check: {color_yes}"
        self.fails_sanity_check_message = f"[%s] Passes sanity check: {color_no}"

    def _print_pass_fail(self, pass_fail: bool, congress_num: str):
        if pass_fail:
            self.logger.info(self.passes_sanity_check_message, congress_num)
        else:
            self.logger.warning(self.fails_sanity_check_message, congress_num)

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
            self.logger.info(
                "[%s] Expected vote count: %s", congress_num, expected_vote_count
            )
            self.logger.info(
                "[%s] Actual vote count: %s", congress_num, actual_vote_count
            )
            self._print_pass_fail(pass_fail, congress_num)
            print("")

    def _run_legislator_sanity_check(self):
        """
        Sanity-check legislators.
        There should be at least 535 legislators in the database.
        """
        legislator_orm = LegislatorOrm()
        actual_legislator_count = legislator_orm.get_count()
        pass_fail = actual_legislator_count >= 535

        self.logger.info("Actual legislator count: %s", actual_legislator_count)
        self._print_pass_fail(pass_fail, "n/a")
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
            self.logger.info(
                "[%s] Checking Congress %s metadata...", congress_num, congress_num
            )
            self._print_pass_fail(pass_fail, congress_num)
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
