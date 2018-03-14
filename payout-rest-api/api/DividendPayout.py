import MySQLdb
import os
from datetime import datetime
from decimal import Decimal


class DividendPayout:

    def __init__(self,
                 exchange_code,
                 security_symbol,
                 declared_date,
                 record_date,
                 ex_date,
                 pay_date,
                 net_amount,
                 currency_code,
                 frequency,
                 payment_type,
                 qualified,
                 flag):
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
        self.qualified = qualified
        self.flag = flag

        self.check_mandatory_fields()
        self.convert_datatypes()

        parms = self.get_db_params()
        # Open database connection
        self.conn = MySQLdb.connect(parms['host'],
                                    parms['user'],
                                    parms['passwd'],
                                    parms['db'],
                                    parms['port']
                                    )
        self.check_security()

    def insert(self):
        cur = self.conn.cursor()
        self.check_duplicate(cur)
        stmt = self.get_insert_payout_stmt()
        cur.execute(stmt)
        self.conn.commit()

    def check_security(self):
        cur = self.conn.cursor()
        stmt = self.get_select_security_stmt()
        cur.execute(stmt, [self.exchange_code, self.security_symbol])
        data = cur.fetchone()
        if data is None:  # if security not found in db
            raise SystemError(
                "Unknown security. exchange_code={} "
                "security_symbol={}".format(
                    self.exchange_code,
                    self.security_symbol
                )
            )
        else:  # save exchange and security unique ids
            self.security_id = data[0]
            self.exchange_id = data[1]

    def check_duplicate(self, cursor):
        res = self.find_payout_close_to_paydate(cursor)
        if res:
            if self.equals(res):
                raise SystemError("Duplicate payout")
            else:
                raise SystemError("Ambiguous payout. May be a duplicate")

    def find_payout_close_to_paydate(self, cursor):
        stmt = self.get_select_payout_paydate_stmt()
        cursor.execute(stmt, [self.security_id, self.pay_date, self.pay_date])
        data = cursor.fetchone()
        if data is not None:
            cols_name = [i[0] for i in cursor.description]
            return dict(zip(cols_name, data))

    def get_insert_payout_stmt(self):
        record = {
            'security_id': str(self.security_id),
            'declared_date': self.declared_date.strftime('%Y-%m-%d'),
            'record_date': self.record_date.strftime('%Y-%m-%d'),
            'ex_date': self.ex_date.strftime('%Y-%m-%d'),
            'pay_date': self.pay_date.strftime('%Y-%m-%d'),
            'net_amount': str(self.net_amount),
            'currency_code': self.currency_code,
            'frequency': self.frequency or 'NULL',
            'payment_type': self.payment_type or 'NULL',
            'qualified': self.qualified or 'NULL',
            'flag': self.flag or 'NULL'
        }
        columns = ', '.join(record.keys())
        values = ', '.join(["'" + value + "'" for value in record.values()])
        return 'INSERT INTO dividend_payout ( {} ) VALUES ( {} )'.format(
            columns,
            values
        )

    @staticmethod
    def get_select_security_stmt():
        return ("SELECT s.security_id, e.exchange_id "
                "FROM security s, exchange e "
                "WHERE s.exchange_id = e.exchange_id "
                "AND e.code=%s AND s.symbol=%s"
                )

    @staticmethod
    def get_select_payout_paydate_stmt():
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

    def check_mandatory_fields(self):
        error_str = "mandatory parameter is missing: '%s'"
        assert self.exchange_code, error_str % "exchange_code"
        assert self.security_symbol, error_str % "security_symbol"
        assert self.declared_date, error_str % "declared_date"
        assert self.record_date, error_str % "record_date"
        assert self.ex_date, error_str % "ex_date"
        assert self.pay_date, error_str % "excpay_datehange_code"
        assert self.net_amount, error_str % "net_amount"
        assert self.currency_code, error_str % "currency_code"

    def convert_datatypes(self):
        self.declared_date = datetime.strptime(
            self.declared_date, '%Y-%m-%d').date()
        self.record_date = datetime.strptime(
            self.record_date, '%Y-%m-%d').date()
        self.ex_date = datetime.strptime(
            self.ex_date, '%Y-%m-%d').date()
        self.pay_date = datetime.strptime(
            self.pay_date, '%Y-%m-%d').date()
        self.net_amount = Decimal(self.net_amount)

    def equals(self, payout_dict):
        for key in payout_dict:
            if payout_dict[key] == 'NULL':
                payout_dict[key] = None

        return self.security_id == payout_dict['security_id'] \
            and self.declared_date == payout_dict['declared_date'] \
            and self.record_date == payout_dict['record_date'] \
            and self.ex_date == payout_dict['ex_date'] \
            and self.pay_date == payout_dict['pay_date'] \
            and self.net_amount == payout_dict['net_amount'] \
            and self.currency_code == payout_dict['currency_code'] \
            and self.frequency == payout_dict['frequency'] \
            and self.payment_type == payout_dict['payment_type'] \
            and self.qualified == payout_dict['qualified'] \
            and self.flag == payout_dict['flag']
