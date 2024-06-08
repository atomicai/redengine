from functools import wraps
from pathlib import Path
from typing import Dict, Any, List
from tdk.user.schemas import RegisterForm, Token
from authlib.integrations.starlette_client import OAuth
import requests
from passlib.context import CryptContext
from google_auth_oauthlib.flow import Flow
from quart import Quart, redirect, url_for, request, jsonify, g
from quart_schema import QuartSchema, validate_request, validate_response
from tdk.prime import verify_token, addUser, loginUser, refresh_user_token, authorize_user, get_all_users, predict_post, \
    delete_account, generate_tokens
from requests_oauthlib import OAuth2Session
import asyncio
import os
import dotenv
import yaml
import rethinkdb as r
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from pip._vendor import cachecontrol
import google.auth.transport.requests

dotenv.load_dotenv()

rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
# client_secrets_file = os.path.join(pathlib.Path(__file__).parent, "client_secret.json")

app = Quart(__name__)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

app.secret_key = os.environ.get("SECRET_KEY")

with open(str(Path(os.getcwd()).parent / "config.yaml"), 'r') as file:
    config = yaml.safe_load(file)

flow = Flow.from_client_config(
    client_config=config,
    scopes=["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email",
            "openid"],
    redirect_uri="http://127.0.0.1:5000/callback"
)

QuartSchema(app)
pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def authorized(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if auth_header:
            token = auth_header.split(" ")[1]
            user_id = await verify_token(token)
            if user_id:
                g.user_id = user_id
                return await f(*args, **kwargs)
        return jsonify({"message": "Unauthorized"}), 401

    return decorated_function


@app.route("/")
def index():
    return 'Welcome to the Quart App! <br><a href="/register">Register</a> <br><a href="/login">Login</a> <br><a href="/register/google">Register with Google</a>'


@app.route('/login', methods=['POST'])
@validate_request(RegisterForm)
def login(data: RegisterForm) -> Dict[str, Any]:
    return asyncio.run(loginUser(data))


@app.route('/register/google')
def register_google() -> redirect:
    authorization_url, state = flow.authorization_url()
    print(authorization_url)
    return redirect(authorization_url)


@app.route('/callback')
def authorize():
    print(request)
    return asyncio.run(authorize_user(request))

@app.route("/registration", methods=["POST"])
@validate_request(RegisterForm)
def registration(data: RegisterForm):
    return asyncio.run(addUser(data))

@app.route("/refresh-token", methods=["POST"])
@validate_request(Token)
def refresh_token(data: Token):
    return asyncio.run(refresh_user_token(data))

@app.route('/users', methods=['GET'])
@authorized
async def get_users() -> List[Dict[str, Any]]:
    return asyncio.run(get_all_users())


@app.route('/delete', methods=['DELETE'])
@authorized
def delete(id):
    return asyncio.run(delete_account(id))


@app.route('/predict', methods=['GET'])
@authorized
async def predict() -> any:
    return asyncio.run(predict_post())


if __name__ == "__main__":
    app.run(debug=True)
