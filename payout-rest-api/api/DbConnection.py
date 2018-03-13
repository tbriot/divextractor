import MySQLdb
import os
from datetime import datetime
from decimal import Decimal


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
        self.validate_payout(payout)
        payout = self.convert_type(payout)

        cur = self.conn.cursor()
        sec = self.find_security(
            cur,
            payout['exchange_code'],
            payout['security_symbol'],
            )

        if sec is None:  # if security was not found in db
            raise SystemError(
                "Unknown security. exchange_code={} "
                "security_symbol={}".format(
                    payout['exchange_code'],
                    payout['security_symbol']
                )
            )

        del payout['exchange_code']
        del payout['security_symbol']
        payout['security_id'] = sec['security_id']
        print(payout)

        p = self.find_payout_close_to_paydate(
                cur,
                sec['security_id'],
                payout['pay_date']
            )

        del p['payout_id']
        print(p)
        if p is not None:  # if payout was found w/ paydate +/- 1 week
            if p == payout:
                raise SystemError("Duplicate payout")

            else:
                raise SystemError("Ambiguous payout. May be a duplicate")

        stmt = self.get_insert_payout_stmt(payout)
        cur.execute(stmt, payout.values())
        self.conn.commit()

    def find_security(self, cursor, exchange_code, security_symbol):
        stmt = self.get_select_security_stmt(exchange_code, security_symbol)
        cursor.execute(stmt, [exchange_code, security_symbol])
        data = cursor.fetchone()
        if data is not None:
            return {'security_id': data[0],
                    'exchange_id': data[1]}

    def find_payout_close_to_paydate(self, cursor, security_id, pay_date):
        stmt = self.get_select_payout_paydate_stmt(security_id, pay_date)
        cursor.execute(stmt, [security_id, pay_date, pay_date])
        data = cursor.fetchone()
        if data is not None:
            cols_name = [i[0] for i in cursor.description]
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

    @staticmethod
    def validate_payout(payout):
        assert payout.get('exchange_code'), \
            "'exchange_code' param is missing"
        assert payout.get('security_symbol'), \
            "'security_symbol' param is missing"
        assert payout.get('pay_date'), \
            "'spay_date' param is missing"

    @staticmethod
    def convert_type(payout):
        payout['declared_date'] = datetime.strptime(payout['declared_date'],
                                                    '%Y-%m-%d').date()
        payout['record_date'] = datetime.strptime(payout['record_date'],
                                                  '%Y-%m-%d').date()
        payout['ex_date'] = datetime.strptime(payout['ex_date'],
                                              '%Y-%m-%d').date()
        payout['pay_date'] = datetime.strptime(payout['pay_date'],
                                               '%Y-%m-%d').date()
        # payout['net_amount'] = Decimal(payout['net_amount'])
        return payout
