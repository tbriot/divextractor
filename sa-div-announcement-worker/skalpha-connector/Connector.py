import requests
from bs4 import BeautifulSoup
from Dividend import Dividend
import re
import json

DECLARED = "DECLARED"
GOES_EX = "GOES_EX"
MONTHLY_DISTRIB = "MONTHLY_DISTRIB"
OTHER = "OTHER"

class Connector:
    headers = {
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0'
    }
    
    def __init__(self, url):
        self.url = url
    
    def get_daily_divs(self, date):
        html = self._get_HTML(date)
        if not html:
            raise SystemError("HTML content not found")

        return self._extract_divs(html)
    
    def _get_HTML(self, date):
        querystring = {
            'current_tag': 'dividends',
            'date': date.strftime('%Y-%m-%d')
        }

        r = requests.get(
            self.url,
            headers=self.headers,
            params=querystring
        )

        if r.status_code != 200:
            raise SystemError(
                "Unexpected HTTP status code ({}) returned by"
                " Seeking Alpha's server".format(r.status_code))

        data = json.loads(r.content)
        # JSON object contain 'none_on_date' key if there's no content
        if data.get('none_on_date'):
            raise SystemError("No results for selected date='{}'".format(date))
        
        html = data.get('market_currents')
        return html

    def _extract_divs(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.find_all("div", class_="media-body")
        
        div_list = []
        for article in articles:
            articleType = self._get_article_type(article)
            if articleType == DECLARED:
                try:
                    div = self._parse_article_declared(article)
                    div_list.append(div)
                except Exception as e:
                    print('Article could not be parsed: ' + str(e) + '!!')
        return div_list

    @staticmethod
    def _parse_article_declared(article):
        div = Dividend()
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
            div.setTickerSymbol(searchDiv.group(1),
                                    searchDiv.group(2))
            div.setAmount(searchDiv.group(3))
            div.setFrequency(searchDiv.group(4))
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
            div.setDates(searchDates.group(1),
                            searchDates.group(2),
                            searchDates.group(3))
        return div

    @staticmethod    
    def _get_article_type(article):
        title = article.find("div", class_="title").a.string

        if re.match("^.* declares .* dividend$", title):
            return DECLARED
        elif re.match("^.* goes ex-dividend tomorrow$", title):
            return GOES_EX
        elif re.match("^.* declares monthly distribution$", title):
            return MONTHLY_DISTRIB
        else:
            return OTHER
