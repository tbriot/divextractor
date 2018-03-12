import MySQLdb


class DbConnection:

    def __init__(self, host, user, passwd, db, port=3306):
        # Open database connection
        self.conn = MySQLdb.connect(host, user, passwd, db, port)

    def insert_payout(self, payout):
        cur = self.conn.cursor()
        stmt = self.get_insert_payout_stmt(payout)
        cur.execute(stmt, payout.values())
        self.conn.commit()

    def find_security(self, exchange_code, security_symbol):
        cur = self.conn.cursor()
        stmt = self.get_select_security_stmt(exchange_code, security_symbol)
        cur.execute(stmt, [exchange_code, security_symbol])
        data = cur.fetchone()
        if data is not None:
            return {'security_id': data[0],
                    'exchange_id': data[1]}

    def find_payout_close_to_paydate(self, security_id, pay_date):
        cur = self.conn.cursor()
        stmt = self.get_select_payout_paydate_stmt(security_id, pay_date)
        cur.execute(stmt, [security_id, pay_date, pay_date])
        data = cur.fetchone()
        if data is not None:
            cols_name = [i[0] for i in cur.description]
            return dict(zip(cols_name, data))

    @staticmethod
    def get_insert_payout_stmt(payout):
        columns = ', '.join(payout.keys())
        placeholders = ', '.join(['%s'] * len(payout))
        return 'INSERT INTO dividend_payout ( {} ) VALUES ( {} )'.format(
            columns,
            placeholders
        )

    @staticmethod
    def get_select_security_stmt(exchange, symbol):
        return ("SELECT s.security_id, e.exchange_id "
                "FROM security s, exchange e "
                "WHERE s.exchange_id = e.exchange_id "
                "AND e.code=%s AND s.symbol=%s"
                )

    @staticmethod
    def get_select_payout_paydate_stmt(security_id, pay_date):
        return ("SELECT * "
                "FROM dividend_payout "
                "WHERE security_id=%s "
                "AND pay_date <= DATE_ADD(%s, INTERVAL 1 WEEK) "
                "AND pay_date >= DATE_ADD(%s, INTERVAL -1 WEEK)"
                )

'''
# prepare a cursor object using cursor() method
cursor = db.cursor()

# execute SQL query using execute() method.
cursor.execute("SELECT VERSION()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()
print("Database version : %s " % data)

# disconnect from server
db.close()
'''
