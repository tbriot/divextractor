import MySQLdb
import os


class DbConnection:

    def __init__(self):
        parms = self.get_db_params()
        # Open database connection
        self.conn = MySQLdb.connect(parms['host'],
                                    parms['user'],
                                    parms['passwd'],
                                    parms['db'],
                                    parms['port']
                                    )

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

    @staticmethod
    def get_db_params():
        try:
            return {
                'host': os.environ['DB_HOST'],
                'port': int(os.environ['DB_PORT']),
                'user': os.environ['DB_USER'],
                'passwd': os.environ['DB_PASSWD'],
                'db': os.environ['DB_SCHEMA']
            }
        except KeyError as e:
            print("Environment variable is not set: %s" % str(e))
