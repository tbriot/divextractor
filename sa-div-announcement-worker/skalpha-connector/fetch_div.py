import datetime
import sys
import setup_logging
import logging
from CliArgParser import CliArgParser
from Connector import Connector
import utils_date as utild
import utils as util


setup_logging.setup_logging()
logger = logging.getLogger('scrape_sa')

def fetch_day(date):
    logger.info(
        "START, extracting dividends, date={}".format(str(date))
    )
    url = util.get_endpoint_url()
    conn = Connector(url)
    cnt = 0
    for div in conn.get_daily_divs(date):
        try:
            # call REST API
            
            logger.debug(
                "Success processing payout, security={}".format(
                    div._data['security']['ticker'])
            )

            cnt += 1
        except Exception as e:
            logger.error(
                "Eerro processing payout, security={}, "
                "messsage={}".format(
                    div._data['security']['ticker'],
                    str(e))
            )

    logger.info(
        "END, extracting dividends, date={}, div_processed={}".format(
            str(date), cnt)
    )

def fetch_window(start_date, end_date):
    print("fetch window. start: {}, end: {}".format(
        str(start_date), str(end_date)))
    for day in utild.daterange(start_date, end_date):
        fetch_day(day)

def main():
    parser = CliArgParser()
    args, mode = parser.parse_args()
    if mode == 'DAY':
        fetch_day(args.date)
    elif mode == 'WINDOW':
        fetch_window(args.start, args.end)
    else:
        fetch_day(datetime.date.today())

if __name__ == '__main__':
    main()
