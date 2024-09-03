from functools import wraps
from pathlib import Path
from typing import Dict, Any, List
from redengine.tdk.user.schemas import Token, userIdChat, RegisterFormTelegram, TgUserPhoto, TgUserInfo, TgUserReaction, GenerationLogin, RegisterForm, Favorites, UserPhoto, UserInfo, Posts, UserReaction
from authlib.integrations.starlette_client import OAuth
from dataclasses import dataclass, asdict
from redengine.tdk.lib.asdict_without_none import asdict_without_none
import requests
import datetime
import jwt
from passlib.context import CryptContext
from quart import Quart, redirect, url_for, request, jsonify, g, send_file
from quart_schema import QuartSchema, validate_request, validate_response
from redengine.tdk.prime import verify_token, loginUser, refresh_user_token, get_all_users, predict_post_tg, delete_account, generate_tokens, authorize_user, start_messaging, select_user, chat, websocket, messages, addTgUser, addTelegramUserPhoto, addTgUserInfo, addTgUserReaction, generateName,addUser, predict_posts, addFavorite, addUserReaction, showFavorites, addUserInfo, addUserPhoto,postsOfTime
from requests_oauthlib import OAuth2Session
import asyncio
import os
import dotenv
import pytz
import yaml
import rethinkdb as r
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from pip._vendor import cachecontrol
import google.auth.transport.requests
from redengine.configuring import Config
dotenv.load_dotenv()

rdb = r.RethinkDB()
conn = rdb.connect(host=Config.app.host, port=28015)

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
    redirect_uri="https://polaroids.ngrok.app/callback"
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
            data = jwt.decode(token,Config.jwt_secret_key , algorithms=["HS256"], leeway=datetime.timedelta(seconds=60*60))
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
    return jsonify({"authorization_url":authorization_url})


@app.route('/callback')
async def authorize():
    return await authorize_user(request)

@app.route("/registration", methods=["POST"])
@validate_request(RegisterForm)
async def registration(data: RegisterForm):
    return await addUser(data)

@app.route("/registration-tg", methods=["POST"])
@validate_request(RegisterFormTelegram)
async def registration_tg(data: RegisterFormTelegram):
    data=asdict(data)
    return await addTgUser(data)

@app.route('/generate_names', methods=['POST'])
@validate_request(GenerationLogin)
async def generate_names(data: GenerationLogin):
    data=asdict(data)
    return await generateName(data)

# @app.route("/add-tg-photo", methods=["POST"])
# @validate_request(TgUserPhoto)
# async def addTgUserPhoto(data: TgUserPhoto):
#     data = asdict_without_none(data)
#     return await addTelegramUserPhoto(data)

# @app.route("/add-tg-userinfo", methods=["POST"])
# @validate_request(TgUserInfo)
# async def addTgUserInformation(data: TgUserInfo):
#     data = asdict_without_none(data)
#     return await addTgUserInfo(data)

# @app.route("/add-user-photo", methods=["POST"])
# @validate_request(UserPhoto)
# async def addTgUserPhoto(data: TgUserPhoto):
#     data = asdict_without_none(data)
#     return await addTelegramUserPhoto(data)

@app.route("/add-userinfo", methods=["POST"])
@validate_request(UserInfo)
async def addUserInformation(user_id, data: UserInfo):
    data = asdict_without_none(data)
    return await addUserInfo(user_id, data)

@app.route("/add-tg-reaction", methods=["POST"])
@validate_request(TgUserReaction)
async def addTelegramUserReaction(data: TgUserReaction):
    data = asdict(data)
    return await addTgUserReaction(data)

@app.route("/add-reaction", methods=["POST"])
@authorized
@validate_request(UserReaction)
async def addReaction(user_id,data: UserReaction):
    data = asdict(data)
    return await addUserReaction(user_id,data)

@app.route('/upload-photo', methods=['POST'])
@authorized
async def upload_photo(user_id):
    photo = await request.files['photo']
    return await addUserPhoto(user_id, photo)
    
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
   
@app.route('/start-messaging', methods=['GET', 'POST'])
@authorized
async def start_chat(user_id):
    return await start_messaging(request,user_id)


@app.route('/select_user', methods=['GET', 'POST'])
@validate_request(RegisterForm)
async def select_user_to_chat():
    return await select_user(request)

@app.route('/count_posts', methods=['GET'])
@authorized
async def count_posts(user_id):
    # Получаем параметры запроса

    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    return await postsOfTime(user_id,start_time,end_time)

    
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

# @app.route('/predict/<int:tg_user_id>', methods=['GET'])
# @authorized
# async def predict() -> any:
#     return asyncio.run(predict_post())

@app.route('/posts', methods=['POST'])
@authorized
@validate_request(Posts)
async def prediction(user_id, data: Posts):
    data = asdict(data)
    return await predict_posts(user_id, data)

@app.route('/predict/<int:tg_user_id>', methods=['GET'])
async def predict(tg_user_id) -> any:
    return await predict_post_tg(tg_user_id)

@app.route('/media/<path:filename>', methods=['GET'])
async def get_media(filename):
    try:
        return await send_file(filename)
    except FileNotFoundError:
        return jsonify({"message": "File not found"}), 404
    
@app.route('/add-favorite', methods=['POST'])
@authorized
@validate_request(Favorites)
async def add_favorite(user_id,data: Favorites):
    data = asdict(data)
    return await addFavorite(user_id, data)
    
@app.route('/show-favorites', methods=['GET'])
@authorized
async def show_favorites(user_id):
    return await showFavorites(user_id)


if __name__ == "__main__":
    app.run(host=Config.app.host, port=8000, debug=True)
