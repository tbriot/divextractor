from collections import defaultdict
import json


class Dividend:
    def __init__(self):
        self._data = defaultdict(dict)

    def setTickerSymbol(self, stockExchange, ticker):
        self._data['security']['stockExchange'] = stockExchange
        self._data['security']['ticker'] = ticker

    def setAmount(self, amount):
        self._data['payout']['amount'] = amount
        self._data['payout']['currency'] = 'TODO'

    def setFrequency(self, freq):
        self._data['payout']['frequency'] = freq

    def setDates(self, payable, record, ex):
        self._data['dates']['payable'] = payable
        self._data['dates']['record'] = record
        self._data['dates']['ex'] = ex

    def getDict(self):
        return self._data

    def toJson(self):
        return json.dumps(self._data)

    def __str__(self):
        return "({}:{}) {} payable on {}".format(
               self._data['security']['stockExchange'],
               self._data['security']['ticker'],
               self._data['payout']['amount'],
               self._data['dates']['payable'])
