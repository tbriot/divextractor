import datetime
import json


class Dividend:
    def __init__(self,
                 exchange_code,
                 security_symbol,
                 declared_date,
                 record_date,
                 ex_date,
                 pay_date,
                 net_amount,
                 currency_code,
                 frequency=None,
                 payment_type=None):
        self.exchange_code = exchange_code
        self.security_symbol = security_symbol
        self.declared_date = declared_date
        self.record_date = record_date
        self.ex_date = ex_date
        self.pay_date = pay_date
        self.net_amount = net_amount
        self.currency_code = currency_code
        self.frequency = frequency
        self.payment_type = payment_type

    def to_json(self):
        dt_format = '%Y-%m-%d'
        d = {
            'declared_date': self.declared_date.strftime(dt_format),
            'record_date': self.record_date.strftime(dt_format),
            'ex_date': self.ex_date.strftime(dt_format),
            'pay_date': self.pay_date.strftime(dt_format),
            'net_amount': self.net_amount,
            'currency_code': self.currency_code,
        }

        if self.frequency:
            d['frequency'] = self.frequency
        if self.payment_type:
            d['payment_type'] = self.payment_type

        return json.dumps(d)

    def __str__(self):
        return "({}:{}) {} {} payable on {}".format(
               self.exchange_code,
               self.security_symbol,
               self.currency_code,
               self.net_amount,
               self.pay_date)
