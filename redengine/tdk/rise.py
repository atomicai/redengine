from functools import wraps
from pathlib import Path
from typing import Dict, Any, List
from redengine.tdk.user.schemas import RegisterForm, Token, userIdChat
from authlib.integrations.starlette_client import OAuth
from dataclasses import dataclass
import requests

import jwt
from passlib.context import CryptContext
from google_auth_oauthlib.flow import Flow
from quart import Quart, redirect, url_for, request, jsonify, g
from quart_schema import QuartSchema, validate_request, validate_response
from redengine.tdk.prime import verify_token, addUser, loginUser, refresh_user_token, get_all_users, predict_post, \
    delete_account, generate_tokens, authorize_user, start_messaging, select_user, chat, websocket, messages
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
from redengine.configuring import Config
dotenv.load_dotenv()

rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
# client_secrets_file = os.path.join(pathlib.Path(__file__).parent, "client_secret.json")

app = Quart(__name__)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

app.secret_key = Config.jwt_secret_key
QuartSchema(app)

with open(str(Path(os.getcwd()) / "config.yaml")) as fp:
    flowConfig= yaml.safe_load(fp)


flow = Flow.from_client_config(
    client_config=flowConfig,
    scopes=["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email",
            "openid"],
    redirect_uri="http://127.0.0.1:5000/callback"
)


pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')

def authorized(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]

        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token,Config.jwt_secret_key , algorithms=["HS256"])
            user_id = data['user_id']
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired!'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Token is invalid!'}), 401

        # Pass the user_id to the endpoint
        return await f(user_id=user_id, *args, **kwargs)
    return decorated_function


@app.route("/googleTest")
async def googleTest():
    return 'Welcome to the Quart App! <br><a href="/register">Register</a> <br><a href="/login">Login</a> <br><a href="/register/google">Register with Google</a>'


@app.route('/login', methods=['POST'])
@validate_request(RegisterForm)
async def login(data: RegisterForm) -> Dict[str, Any]:
    return await loginUser(data)


@app.route('/register/google')
async def register_google() -> redirect:
    authorization_url, state = flow.authorization_url()
    print(authorization_url)
    return redirect(authorization_url)


@app.route('/callback')
async def authorize():
    return await authorize_user(request)

@app.route("/registration", methods=["POST"])
@validate_request(RegisterForm)
async def registration(data: RegisterForm):
    return await addUser(data)

@app.route("/refresh-token", methods=["POST"])
@validate_request(Token)
async def refresh_token(data: Token):
    return await refresh_user_token(data)

@app.route('/users', methods=['GET'])
@authorized
async def get_users() -> List[Dict[str, Any]]:
    return asyncio.run(get_all_users())


@app.route('/delete', methods=['DELETE'])
@authorized
async def delete(user_id):
    return await delete_account(user_id)
   
@app.route('/start_messaging', methods=['GET', 'POST'])
@authorized
async def start_chat(user_id):
    return await start_messaging(request,user_id)


@app.route('/select_user', methods=['GET', 'POST'])
@validate_request(RegisterForm)
async def select_user_to_chat():
    return await select_user(request)
    
@app.route('/chat', methods=['GET'])
@authorized
async def talk(user_id):
    return await chat(user_id)
    
@app.route('/messages', methods=['GET'])
@authorized
async def get_messages(user_id):
    return await messages(request,user_id)

@app.websocket('/ws')
@authorized
async def ws(user_id):
    return await websocket(user_id)

@app.route('/predict', methods=['GET'])
@authorized
async def predict() -> any:
    return asyncio.run(predict_post())


if __name__ == "__main__":
    app.run(debug=True)
