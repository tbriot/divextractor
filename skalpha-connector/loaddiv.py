from pymongo import MongoClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HOST = '192.168.99.100'
PORT = 27017
DB = 'dividend'
COLL_IMPORT = 'import'
COLL_DIV = 'dividend'

PENDING = 'PENDING'
INPROGRESS = 'INPROGRESS'
IGNORED = 'IGNORED'
INSERTED = 'INSERTED'
AMBIGUOUS = 'AMBIGUOUS'

client = MongoClient(HOST, PORT)
db = client[DB]
importedDivs = db[COLL_IMPORT]
dividends = db[COLL_DIV]


def loadAll():
    logger.info('Start reading entries from IMPORT collection')
    count = 0
    while True:
        div = getNextDiv()
        if div is not None:
            load(div)
            count += 1
        else:
            break
    logger.info('Load complete. {} entries were processed'.format(count))


def determineProcessStatus(div):
    existingDivs = dividends.find(
        {'security.stockExchange': div['security']['stockExchange'],
         'security.ticker': div['security']['ticker']}
    )

    bestMatch = {'score': 0}
    for existingDiv in existingDivs:
        dupScore = getDupScore(existingDiv, div)
        if dupScore > bestMatch['score']:
            bestMatch = {'_id': existingDiv['_id'], 'score': dupScore}

    if bestMatch['score'] == 100:
        return IGNORED
    elif bestMatch['score'] < 50:
        return INSERTED
    else:
        return AMBIGUOUS


def getDupScore(existingDiv, div):
    dupScore = 0
    if existingDiv['dates']['payable'] == div['dates']['payable']:
        dupScore += 40
    if existingDiv['dates']['record'] == div['dates']['record']:
        dupScore += 20
    if existingDiv['dates']['ex'] == div['dates']['ex']:
        dupScore += 20
    if existingDiv['payout']['amount'] == div['payout']['amount']:
        dupScore += 10
    if existingDiv['payout']['currency'] == div['payout']['currency']:
        dupScore += 5
    if existingDiv['payout']['frequency'] == div['payout']['frequency']:
        dupScore += 5
    return dupScore


def getNextDiv():
    return importedDivs.find_one_and_update(
            {'status': PENDING},
            {
                '$set': {'status': INPROGRESS},
                '$currentDate': {
                    'lastModified': True
                }
            }
    )


def load(div):
    action = determineProcessStatus(div)
    if action == INSERTED:
        dividends.insert_one(reformat(div))
    importedDivs.update(
        {'_id': div['_id']},
        {
            '$set': {'status': action},
            '$currentDate': {
                'lastModified': True
            }
        }
    )
    logger.info('Processed entry ({}:{}). Status={}'.format(
            div['security']['stockExchange'],
            div['security']['ticker'],
            action
        )
    )


def reformat(div):
    newDiv = div.copy()
    del newDiv['_id']
    del newDiv['status']
    newDiv['created'] = datetime.utcnow()
    return newDiv


loadAll()
