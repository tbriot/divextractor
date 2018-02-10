class DividendPayout:
    def __init__(self):
        pass

    def setTickerSymbol(self, stockExchange, symbol):
        self.stockExchange = stockExchange
        self.symbol = symbol

    def setAmount(self, amount):
        self.amount = amount

    def setFrequency(self, freq):
        self.freq = freq

    def setDates(self, payable, record, ex):
        self.payable = payable
        self.record = record
        self.ex = ex

    def __str__(self):
        return "({}:{}) {} - {} - payable={} -"\
               " onRecord={}".format(self.stockExchange or "N/A",
                                     self.symbol or "N/A",
                                     self.amount or "N/A",
                                     self.freq or "N/A",
                                     self.payable or "N/A",
                                     self.record or "N/A")
