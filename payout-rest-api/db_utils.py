import os


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
            raise Exception(
                "Mandatory environment variable is not set: %s" % str(e))
