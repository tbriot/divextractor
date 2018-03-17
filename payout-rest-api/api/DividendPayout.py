import MySQLdb
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from customExceptions import ValidationException, DbConnectionException, \
    DuplicateException, ConflictException


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
        try:
            self.conn = MySQLdb.connect(
                parms['host'],
                parms['user'],
                parms['passwd'],
                parms['db'],
                parms['port'],
                connect_timeout=parms['timeout']
            )
        except Exception:
            raise DbConnectionException("Error while connecting to db")
        self.check_security()

    def insert(self):
        cur = self.conn.cursor()
        self.check_duplicate(cur)
        stmt = self.get_insert_payout_stmt()
        try:
            cur.execute(stmt)
            self.conn.commit()
            return cur.lastrowid
        except Exception:
            raise DbConnectionException("Error while inserting record in db")

    def check_security(self):
        cur = self.conn.cursor()
        stmt = self.get_select_security_stmt()
        try:
            cur.execute(stmt, [self.exchange_code, self.security_symbol])
        except Exception:
            raise DbConnectionException("Error while querying the db")
        data = cur.fetchone()
        if data is None:  # if security not found in db
            raise ValidationException(
                "Unknown security. exchange_code={}. "
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
                raise DuplicateException("Duplicate payout")
            else:
                msg_templ = "May be a duplicate of payout_id={}. " \
                            "No payout was inserted."
                raise ConflictException(msg_templ.format(res['payout_id']))

    def find_payout_close_to_paydate(self, cursor):
        stmt = self.get_select_payout_paydate_stmt()
        try:
            cursor.execute(stmt, [self.security_id,
                                  self.pay_date,
                                  self.pay_date])
        except Exception:
            raise DbConnectionException("Error while queying the db")
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
                'user': os.environ['DB_USER'],
                'passwd': os.environ['DB_PASSWD'],
                'db': os.environ['DB_SCHEMA'],
                # default port is 3306 if not specified in env var
                'port': int(os.environ.get('DB_PORT') or 3306),
                # default connection timeout is 5 seconds i
                # if not specified in env var
                'timeout': int(os.environ.get('DB_TIMEOUT') or 5)
            }
        except KeyError as e:
            print("Mandatory environment variable is not set: %s" % str(e))

    def check_mandatory_fields(self):
        self.raise_except_if_none('exchange_code', self.exchange_code)
        self.raise_except_if_none('security_symbol', self.security_symbol)
        self.raise_except_if_none('declared_date', self.declared_date)
        self.raise_except_if_none('record_date', self.record_date)
        self.raise_except_if_none('ex_date', self.ex_date)
        self.raise_except_if_none('pay_date', self.pay_date)
        self.raise_except_if_none('net_amount', self.net_amount)
        self.raise_except_if_none('currency_code', self.currency_code)

    @staticmethod
    def raise_except_if_none(key, value):
        if not value:
            raise ValidationException(
                "Mandatory field={} is missing".format(key))

    def convert_datatypes(self):
        self.declared_date = self.parse_date('declared_date',
                                             self.declared_date)
        self.record_date = self.parse_date('record_date',
                                           self.record_date)
        self.ex_date = self.parse_date('ex_date',
                                       self.ex_date)
        self.pay_date = self.parse_date('pay_date',
                                        self.pay_date)
        self.net_amount = self.parse_decimal('net_amount', self.net_amount)

    @staticmethod
    def parse_date(key, value):
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except ValueError:
            raise ValidationException(
                "input={} date format is invalid. "
                "Expected format is 'YYYY-MM-DD'. "
                "received_date={}".format(key, value))

    @staticmethod
    def parse_decimal(key, value):
        try:
            return Decimal(value)
        except InvalidOperation:
            raise ValidationException(
                "input={} decimal format is invalid. "
                "Expected format is XX.YYY. "
                "received_decimal={}".format(key, value))

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
