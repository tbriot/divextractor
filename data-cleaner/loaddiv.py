from pymongo import MongoClient
from datetime import datetime
import logging
import setup_logging
from collections import defaultdict
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# HOST = '192.168.99.100'
# PORT = 27017
HOST = os.environ['MONGODB_HOST']
PORT = int(os.environ['MONGODB_PORT'])
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

setup_logging.setup_logging()
logger = logging.getLogger('loaddiv')


def loadAll():
    logger.info(
        'START loading dividends db={} from_collection={}'
        ' to_collection={}'.format(
            DB,
            COLL_IMPORT,
            COLL_DIV
        )
    )
    results = defaultdict(int)
    results['found_records'] = False
    while True:
        div = getNextDiv()
        if div is not None:
            logger.debug('Read dividend=' + str(div))
            results['found_records'] = True
            action = load(div)
            results[action] += 1
        else:
            break
    logger.info('END loading dividends. results=' + str(results))


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
    logger.debug('Process status is action={} for div_ticker={}'.format(
        action,
        div['security']['ticker']
    ))
    if action == INSERTED:
        logger.debug(
            'Inserting entry div_ticker={} payable={} db={} '
            'to_collection={}'.format(
                div['security']['ticker'],
                div['dates']['payable'],
                DB,
                COLL_DIV
            )
        )
        dividends.insert_one(reformat(div))
        logger.debug(
            'Inserted entry div_ticker={} payable={} db={} '
            'to_collection={}'.format(
                div['security']['ticker'],
                div['dates']['payable'],
                DB,
                COLL_DIV
            )
        )

    logger.debug(
        'Updating entry div_ticker={} payable={} db={} '
        'from_collection={}'.format(
            div['security']['ticker'],
            div['dates']['payable'],
            DB,
            COLL_IMPORT
        )
    )
    importedDivs.update(
        {'_id': div['_id']},
        {
            '$set': {'status': action},
            '$currentDate': {
                'lastModified': True
            }
        }
    )
    logger.debug(
        'Updated entry div_ticker={} payable={} db={} '
        'from_collection={} action={}'.format(
            div['security']['ticker'],
            div['dates']['payable'],
            DB,
            COLL_IMPORT,
            action
        )
    )
    return action


def reformat(div):
    newDiv = div.copy()
    del newDiv['_id']
    del newDiv['status']
    newDiv['created'] = datetime.utcnow()
    return newDiv


loadAll()
