from DbConnection import DbConnection

'''
host = 'mysqlinstance.co0pqf5yoscl.ca-central-1.rds.amazonaws.com'
user = 'tbriot'
passwd = 'irondesK8'
db = 'dividend'
'''

payout = {
    'exchange_code': 'NASDAQ',
    'security_symbol': 'ATVI',
    'declared_date': '2018-03-12',
    'record_date': '2018-04-02',
    'ex_date': '2018-04-03',
    'pay_date': '2018-04-23',
    'net_amount': 0.25,
    'currency_code': 'USD',
    'frequency': 'MONTHLY',
    'payment_type': None,
    'qualified': None,
    'flag': None
}

conn = DbConnection()
res = conn.insert_payout(payout)
# conn.insert_payout(payout)
