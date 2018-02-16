from pymongo import MongoClient
from datetime import datetime

PENDING = 'PENDING'


class PersistClient:
    HOST = '192.168.99.100'
    PORT = 27017
    DB = 'dividend'
    COLLECTION = 'import'

    def __init__(self):
        self.client = MongoClient(self.HOST, self.PORT)

    def insert(self, dic):
        dic['status'] = PENDING
        dic['created'] = datetime.utcnow()
        dic['lastModified'] = ''
        db = self.client[self.DB]
        db[self.COLLECTION].insert_one(dic)
