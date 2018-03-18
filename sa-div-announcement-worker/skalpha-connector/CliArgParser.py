import argparse
from datetime import datetime


class CliArgParser(argparse.ArgumentParser):
    def __init__(self):
        descr = "This program extracts dividend payouts information from " \
                    "the Seeking Alpha website and saves it in a database"
        super().__init__(self, description=descr)

        self.add_argument(
            '--date',
            '-d',
            help="Dividend payouts announced on this day are extracted. "
                 "--start and --end arguments are not passed in this case. "
                 "format YYYY-MM-DD",
            type=self.valid_date,
            required=False)

        self.add_argument(
            '--start',
            '-s',
            help="start date. format YYYY-MM-DD",
            type=self.valid_date,
            required=False)

        self.add_argument(
            '--end',
            '-e',
            help="end date. format YYYY-MM-DD",
            type=self.valid_date,
            required=False)

    def parse_args(self):
        args = super().parse_args()
        if not(args.date or args.start or args.end):
            return args, 'CURRENT_DAY'
        elif args.date and not(args.start or args.end):
            return args, 'DAY'
        elif not(args.date) and args.start and args.end:
            if args.start < args.end:
                return args, 'WINDOW'
            else:
                raise argparse.ArgumentTypeError(
                    "the --start argument should be older than --end one")
        else:
            raise argparse.ArgumentTypeError(
                "either --date argument should be provided or "
                "both --start and --end arguments")

    @staticmethod
    def valid_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            msg = "Not a valid date: '{0}'.".format(s)
            raise argparse.ArgumentTypeError(msg)
