from DividendPayout import DividendPayout
from flask import Flask, request, make_response
import json
from customExceptions import ValidationException

api_name = 'dividend'
api_version = 1
base_url = '/{name}/v{version}'.format(name=api_name, version=api_version)

app = Flask(__name__)


@app.route(base_url + '/payout/<string:exchange_code>'
                      '/<string:security_symbol>',
           methods=['POST'])
def hello(exchange_code, security_symbol):
    r = json.loads(request.data)
    payout = DividendPayout(
        exchange_code=exchange_code,
        security_symbol=security_symbol,
        declared_date=r.get('declared_date'),
        record_date=r.get('record_date'),
        ex_date=r.get('ex_date'),
        pay_date=r.get('pay_date'),
        net_amount=r.get('net_amount'),
        currency_code=r.get('currency_code'),
        frequency=r.get('frequency'),
        payment_type=r.get('payment_type'),
        qualified=r.get('qualified'),
        flag=r.get('flag')
    )
    payout.insert()
    return "Success"


@app.errorhandler(ValidationException)
def handle_error(e):
    resp = make_response(json.dumps({'error_message': str(e)}), 400)
    resp.headers['Content-Type'] = 'application/json'
    return resp
