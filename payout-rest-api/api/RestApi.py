from DividendPayout import DividendPayout

payout = DividendPayout(
    exchange_code='NYSE',
    security_symbol='BRK.B',
    declared_date='2018-03-01',
    record_date='2018-04-02',
    ex_date='2018-04-03',
    pay_date='2018-05-11',
    net_amount=1.25,
    currency_code='CAD',
    frequency='MONTHLY',
    payment_type=None,
    qualified=None,
    flag=None
)

payout.insert()
