import requests
from bs4 import BeautifulSoup
from Dividend import Dividend
import re
import json
import exceptions as ex
from datetime import datetime

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

        return self._extract_divs(html, date)
    
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

    def _extract_divs(self, html, date):
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.find_all("div", class_="media-body")
        
        div_list = []
        for article in articles:
            articleType = self._get_article_type(article)
            if articleType == DECLARED:
                try:
                    div = Connector._parse_article_declared(article, date)
                    div_list.append(div)
                except Exception as e:
                    print('Article could not be parsed: ' + str(e) + '!!')
        return div_list

    @staticmethod
    def _parse_article_declared(article, declared_date):
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
        #
        # "Visa (NYSE:V) declares $0.21/share quarterly dividend,"\
        # " 7.7% increase from prior dividend of $0.195."
        regex = r"\(([\w\.]+):([\w\.]+)\) declares " \
                r"([A-Z$]*)\s?(\d*\.?\d*)/share ([\w\-]+) dividend"

        searchDiv = re.search(regex, bulletList[0].get_text())
        if searchDiv:
            exchange_code = searchDiv.group(1)
            security_symbol = searchDiv.group(2)
            currency = searchDiv.group(3)
            net_amount = searchDiv.group(4)
            frequency = searchDiv.group(5)
        else:
            raise ex.ParsingError(
                bulletList[0].get_text(),
                regex,
                "Could not extract exchange, ticker, amount or frequency")
        # The LAST bullet of the article provides the following information :
        # - payable date, e.g. "Feb. 15", "March 15", "April 12"
        # - record date, e.g. "Feb. 15", "March 15", "April 12"
        # - ex-dividend date, e.g. "Feb. 15", "March 15", "April 12"
        # the last item of the list is extracted with: some_list[-1]        
        
        regex = r"Payable (.*); for shareholders of record (.*); ex-div (.*)\.$"
        searchDates = re.search(regex, bulletList[-1].get_text())
        if searchDates:
            pay_date = searchDates.group(1)
            record_date = searchDates.group(2)
            ex_date = searchDates.group(3)
        else:
            raise ex.ParsingError(
                bulletList[2].get_text(),
                regex,
                "Could not extract pay date, record date or ex date"
                ", security=({}:{})".format(exchange_code, security_symbol))

        return Dividend(
            exchange_code=exchange_code,
            security_symbol=security_symbol,
            declared_date=declared_date,
            record_date=Connector._convert_date_format(record_date, declared_date),
            ex_date=Connector._convert_date_format(ex_date, declared_date),
            pay_date=Connector._convert_date_format(pay_date, declared_date),
            net_amount=net_amount,
            currency_code=Connector._convert_currency_code(currency),
            frequency=Connector._convert_frequency_code(frequency))

    @staticmethod
    def _convert_date_format(date_str, declared_date):
        """ date_str is a dividend record date, ex date or pay date 
            it does NOT contain the year e.g. 'May 9', 'December 12'
            The year has to be guessed based on the declaration date :
            either the same year, or the next year, but it has to be 
            in the future """
        dt_format = '%B %d'
        d = datetime.strptime(date_str, dt_format).date()
        # taking a guess, supposing the input date is the same year
        # as the declaration date
        d = d.replace(year=declared_date.year)
        if declared_date < d: 
            return d
        else: # input date is actually next year
            return d.replace(year=declared_date.year + 1)

    @staticmethod
    def _convert_currency_code(cur):
        if cur == "$":
            return 'USD'
        else:
            return cur

    @staticmethod
    def _convert_frequency_code(freq):
        map = {
            "monthly": 'MONTHLY',
            "quaterly": 'QUATERLY',
            "semi-annual": 'BIANNUALLY',
            "annual": 'ANNUALLY'
        }

        return map.get(freq)

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
