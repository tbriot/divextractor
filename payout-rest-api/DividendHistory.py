import db_utils
import MySQLdb
from customExceptions import DbConnectionException


class DividendHistory():
    def __init__(self):
        parms = db_utils.get_db_params()
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

    # dummy query for healthcheck purpose
    def health_check(self):
        cur = self.conn.cursor()
        stmt = "SELECT * from  dividend_payout where 1=2"
        try:
            cur.execute(stmt)
        except Exception:
            raise DbConnectionException(
                "Error while querying 'dividend_payout' table")
