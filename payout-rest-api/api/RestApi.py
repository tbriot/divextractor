from DbConnection import DbConnection

'''
host = 'mysqlinstance.co0pqf5yoscl.ca-central-1.rds.amazonaws.com'
user = 'tbriot'
passwd = 'irondesK8'
db = 'dividend'
'''

payout = {
    'exchange': 'NYSE',
    'symbol': 'MMM',
    'declared_date': '2018-03-12',
    'record_date': '2018-04-02',
    'ex_date': '2018-04-03',
    'pay_date': '2018-04-23',
    'net_amount': 0.25,
    'currency_code': 'USD',
    'frequency': 'MONTHLY'
}

conn = DbConnection()
res = conn.find_payout_close_to_paydate(2, '2018-04-20')
if res is not None:
    print("Entry with close paydate found")
    del res['payout_id']
    print(res)
# res = conn.find_security(payout['exchange'], payout['symbol'])
# conn.insert_payout(payout)
