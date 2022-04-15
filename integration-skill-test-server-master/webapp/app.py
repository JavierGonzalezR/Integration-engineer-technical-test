import os
import random
from functools import wraps

from constants.json_schema import json_schema, merchant_schema
from constants.responses import merchants_data
from flask import Flask, abort, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from jsonschema import exceptions as jsonschema_exceptions
from jsonschema import validate as jsonschema_validate

app = Flask(__name__)
GRAND_TYPE = ["client_credentials", "refresh_token"]
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
TOKEN_VALUES = os.environ.get("TOKEN_VALUES")
TOKEN_OPTIONS = list(TOKEN_VALUES)
RICHARD_ID = os.environ.get("RICHARD_ID")
BEAUTY_ID = os.environ.get("BEAUTY_ID")
TOKEN_LENGTH = 20

limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["10000 per day", "2000 per hour"]
)


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


@app.errorhandler(401)
def unauthorized(e):
    return jsonify(error=str(e)), 401


@app.errorhandler(400)
def bad_request(e):
    return jsonify(error=str(e)), 400


def generate_access_token():
    token = ""
    for idx in range(TOKEN_LENGTH):
        token += random.choice(TOKEN_OPTIONS)
    return token


def is_valid_token(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('token')
        if not token:
            abort(401, description="")
        try:
            token = token.split("Bearer ")[1]
        except IndexError:
            abort(401, description="")
        token_list = list(token)
        for value in token_list:
            if value not in TOKEN_VALUES:
                abort(401, description="")
        if len(token) != TOKEN_LENGTH:
            abort(401, description="")
        return f(*args, **kwargs)

    return decorated_function


@app.route('/oauth/token', methods=["POST"])
@limiter.limit("1/second", override_defaults=False)
def get_token():
    grant_type = request.args.get('grant_type')
    client_id = request.args.get('client_id')
    client_secret = request.args.get('client_secret')
    expected_grand_type = GRAND_TYPE
    expected_client_id = CLIENT_ID
    expected_client_secret = CLIENT_SECRET
    if not (grant_type and client_id and client_secret):
        abort(401, description="")
    if grant_type in expected_grand_type and client_id == expected_client_id and client_secret == expected_client_secret:
        response_token = {
            "access_token": generate_access_token(),
            "token_type": "bearer",
            "expires_in": 2592000,
            "refresh_token": generate_access_token(),
        }
        return response_token, 200
    abort(401, description="")


@app.route('/api/products', methods=["POST"])
@is_valid_token
def product():
    json_data = request.get_json()
    try:
        jsonschema_validate(json_data, json_schema)
        if json_data["merchant_id"] != RICHARD_ID:
            raise abort(400, description="")
    except jsonschema_exceptions.ValidationError as e:
        raise abort(400, description=str(e))
    return json_data, 200


@app.route('/api/merchants', methods=["GET"])
@is_valid_token
def merchants():
    return merchants_data, 200


@app.route('/api/merchants/<merchant_id>', methods=["PUT", "DELETE"])
@is_valid_token
def update_merchant(merchant_id):
    if request.method == 'PUT':
        json_data = request.get_json()
        try:
            jsonschema_validate(json_data, merchant_schema)
        except jsonschema_exceptions.ValidationError as e:
            raise abort(400, description=str(e))
        if merchant_id == json_data["id"] == RICHARD_ID:
            return json_data, 200
    else:
        if merchant_id == BEAUTY_ID:
            return "", 200
    raise abort(400, description="")


@app.route("/ping")
@limiter.exempt
def ping():
    return "PONG", 200


@app.route("/")
@limiter.exempt
def index():
    return "Skill test server. <a href=\"https://documenter.getpostman.com/view/1992239/TVmMgxxp\" " \
           "target=\"_blank\">API docs</a>", 200


if __name__ == '__main__':
    app.run(host='0.0.0.0')
