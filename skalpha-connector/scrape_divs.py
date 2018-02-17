import requests
import uuid
import json
from bs4 import BeautifulSoup
import re
from dividend import DividendPayout
import sys
from PersistClient import PersistClient
import setup_logging
import logging


DECLARED = "DECLARED"
GOES_EX = "GOES_EX"
MONTHLY_DISTRIB = "MONTHLY_DISTRIB"
OTHER = "OTHER"

URL = 'https://seekingalpha.com/market-news/ajax_get_market_currents'
headers = {
    'Cache-Control': 'no-cache',
    'User-Agent': 'Mozilla/5.0'
}
querystring = {'current_tag': 'dividends'}


setup_logging.setup_logging()
logger = logging.getLogger('scrape_sa')


def scrape_divs(mydate):
    assert validateDateFormat
    querystring['date'] = mydate
    logger.debug('Retrieving web page')
    logger.debug('URL={}, headers={}, querystring={}'.format(
        URL, headers, str(querystring)))
    r = requests.get(URL, headers=headers, params=querystring)
    if r.status_code != 200:
        raise SystemError("Unexpected HTTP status code ({}) returned by"
                          " Seeking Alpha's server".format(r.status_code))

    logger.debug('Page retrieved')
    data = json.loads(r.content)
    html = data.get('market_currents')
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all("div", class_="media-body")

    client = PersistClient()

    cntSuccess = 0
    cntError = 0
    for article in articles:
        articleType = getArticleType(article)
        if articleType == DECLARED:
            try:
                divPayout = parseDeclaredArticle(article)
                logger.debug('Extracted dividend payout: ' + str(divPayout))
                logger.debug('Inserting dividend payout in db')
                client.insert(divPayout.getDict())
                logger.debug('Insertion successful')
                cntSuccess += 1
            except Exception as e:
                logger.error('Article could not be parsed: ' + str(e) + '!!')
                cntError += 1
    return cntSuccess, cntError


def validateDateFormat(date):
    return True


def getArticleType(article):
    title = article.find("div", class_="title").a.string

    if re.match("^.* declares .* dividend$", title):
        return DECLARED
    elif re.match("^.* goes ex-dividend tomorrow$", title):
        return GOES_EX
    elif re.match("^.* declares monthly distribution$", title):
        return MONTHLY_DISTRIB
    else:
        return OTHER


def parseDeclaredArticle(article):
    divPayout = DividendPayout()
    bulletList = article.find("div", class_="bullets").ul.find_all("li")
    # The first bullet of the article provides the following information :
    # - stock exchange. e.g. "NYSE", "NASDAQ", NYSEMKT"
    # - stock ticker symbol. e.g. "AAPL"
    # - dividend payout amount, e.g. "$0.045"
    # - dividend payment frequency, e.g. "monthly", "quaterly",
    #                                    "semi-annual"
    #
    # Samples:
    # "Royal Dutch Shell (NYSE:RDS.A) declares $0.94/share quarterly"\
    # "dividend, in line with previous."
    # "Visa (NYSE:V) declares $0.21/share quarterly dividend,"\
    # " 7.7% increase from prior dividend of $0.195."
    searchDiv = re.search(r"\(([\w\.]+):([\w\.]+)\) declares "
                          r"(.*)/share ([\w\-]+) dividend",
                          bulletList[0].get_text())
    if searchDiv:
        divPayout.setTickerSymbol(searchDiv.group(1),
                                  searchDiv.group(2))
        divPayout.setAmount(searchDiv.group(3))
        divPayout.setFrequency(searchDiv.group(4))
    else:
        raise SystemError("Could not parse string: " +
                          bulletList[0].get_text())
    # The third bullet of the article provides the following information :
    # - payable date, e.g. "Feb. 15", "March 15", "April 12"
    # - record date, e.g. "Feb. 15", "March 15", "April 12"
    # - ex-dividend date, e.g. "Feb. 15", "March 15", "April 12"
    searchDates = re.search(r"Payable (.*);"
                            r" for shareholders of record (.*);"
                            r" ex-div (.*)\.$", bulletList[2].get_text())
    if searchDates:
        divPayout.setDates(searchDates.group(1),
                           searchDates.group(2),
                           searchDates.group(3))
    return divPayout


def main(argv):
    date = argv[0]
    logger.info('START scraping dividends for date={}'.format(date))
    cntSuccess, cntError = scrape_divs(date)
    logger.info(
        'END scraping. Dividend payouts extracted successfully: div_success={}'
        ' and in error: div_error={}'.format(str(cntSuccess), str(cntError)))


if __name__ == '__main__':
    main(sys.argv[1:])
