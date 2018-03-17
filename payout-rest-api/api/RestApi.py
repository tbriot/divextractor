from DividendPayout import DividendPayout
from flask import Flask, request, make_response
import json
from customExceptions import ValidationException, DbConnectionException, \
    DuplicateException, ConflictException

api_name = 'dividend'
api_version = 1
base_url = '/{name}/v{version}'.format(name=api_name, version=api_version)

app = Flask(__name__)


@app.route(base_url + '/payout/<string:exchange_code>'
                      '/<string:security_symbol>',
           methods=['POST'])
def hello(exchange_code, security_symbol):
    try:
        r = json.loads(request.data)
    except ValueError:
        raise ValidationException("JSON document in request is invalid")
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
    resp_body = dict()
    resp_body['payout_id'] = payout.insert()
    resp = make_response(json.dumps(resp_body), 201)
    resp.headers['Content-Type'] = 'application/json'
    return resp


@app.errorhandler(404)
def not_found(e):
    resp = make_response(json.dumps(
        {'error_message': "The requested resource could not be found. "
                          "Check the requested URI."}),
        404
    )
    resp.headers['Content-Type'] = 'application/json'
    return resp


@app.errorhandler(ValidationException)
def handle_valid_error(e):
    return built_error_response(str(e), 400)


@app.errorhandler(DuplicateException)
def handle_duplicate_errror(e):
    return "", 200


@app.errorhandler(ConflictException)
def handle_conflict_errror(e):
    return built_error_response(str(e), 409)


@app.errorhandler(Exception)
def handle_other_error(e):
    return built_error_response("unexpected server error", 500)


def built_error_response(error_msg, code):
    resp = make_response(json.dumps(
        {'error_message': error_msg}),
        code
    )
    resp.headers['Content-Type'] = 'application/json'
    return resp
