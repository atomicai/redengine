from functools import wraps
from pathlib import Path
from typing import Dict, Any, List
from redengine.tdk.user.schemas import (
    Token,
    userIdChat,
    RegisterFormTelegram,
    TgUserPhoto,
    TgUserInfo,
    TgUserReaction,
    GenerationLogin,
    RegisterForm,
    Favorites,
    UserPhoto,
    UserInfo,
    Posts,
    UserReaction,
    KeyPhrases,
    Schemas
)
from authlib.integrations.starlette_client import OAuth
from dataclasses import dataclass, asdict
from redengine.tdk.lib.asdict_without_none import asdict_without_none
import requests
import datetime
import jwt
from passlib.context import CryptContext
from quart import Quart, redirect, url_for, request, jsonify, g, send_file
from quart_schema import QuartSchema, validate_request, validate_response
from redengine.tdk.prime import (
    verify_token,
    loginUser,
    refresh_user_token,
    removeFavorite,
    topPosts,
    get_all_users,
    addReaction,
    predict_post_tg,
    delete_account,
    listReaction,
    addFileServer,
    generate_tokens,
    randomWord,
    authorize_user,
    start_messaging,
    select_user,
    chat,
    websocket,
    messages,
    addTgUser,
    addTelegramUserPhoto,
    removeReaction,
    addTgUserInfo,
    addTgUserReaction,
    generateName,
    addUser,
    predict_posts,
    addFavorite,
    addUserReaction,
    showFavorites,
    addUserInfo,
    addUserPhoto,
    postsOfTime,
    showKeyphrases
)
from requests_oauthlib import OAuth2Session
from redengine.storing.rethinkDb import IReDocStore
import asyncio
import os
import dotenv
import pytz
import yaml
from redis import asyncio as aioredis
import google.auth.transport.requests
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from pip._vendor import cachecontrol
from redengine.configuring import Config


app = Quart(__name__)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

app.secret_key = Config.jwt_secret_key
QuartSchema(app)

RethinkDb = IReDocStore()

with open(str(Path(os.getcwd()) / "config.yaml")) as fp:
    Config = yaml.safe_load(fp)

@app.before_serving
async def before_serving():
    await RethinkDb.connect()

@app.after_serving
async def after_serving():
    await RethinkDb.close()


flow = Flow.from_client_config(
    client_config=Config,
    scopes=[
        "https://www.googleapis.com/auth/userinfo.profile",
        "https://www.googleapis.com/auth/userinfo.email",
        "openid",
    ],
    redirect_uri="https://polaroids.ngrok.app/callback",
)


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def get_redis_connection():
    return await aioredis.from_url('redis://localhost')

def rate_limit():
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            redis = await get_redis_connection()
            user_ip = request.remote_addr
            key = f"rate_limit:{user_ip}"

            current_requests = await redis.get(key)
            current_requests = int(current_requests) if current_requests else 0

            if current_requests >= Config['rate_limit']['limit']:
                return jsonify({"error": "Too many requests"}), 429

            await redis.incr(key)
            await redis.expire(key, Config['rate_limit']['period'])

            await redis.close()

            return await func(*args, **kwargs)
        return wrapper
    return decorator


def brute_force_protection():
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            redis = await get_redis_connection()
            user_ip = request.remote_addr
            key = f"login_attempts:{user_ip}"

            attempts = await redis.get(key)
            attempts = int(attempts) if attempts else 0


            if attempts >= Config['rate_limit']['limit']:
                await redis.close()
                return jsonify({"error": "Too many failed login attempts. Try again later."}), 429

            response = await func(*args, **kwargs)
            response_body, status_code = response

            if status_code == 401:
                await redis.incr(key)
                await redis.expire(key, Config['rate_limit']['period'])

            elif status_code == 200:
                await redis.delete(key)

            await redis.close()
            return response
        return wrapper
    return decorator

@app.route('/test', methods=['GET'])
@rate_limit()
async def test_route():
    return jsonify({"message": "This is a test route with rate limiting."})

def authorized(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        token = None
        if "Authorization" in request.headers:
            token = request.headers["Authorization"].split(" ")[1]

        if not token:
            return jsonify({"message": "Token is missing!"}), 401

        try:
            data = jwt.decode(
                token,
                Config.jwt_secret_key,
                algorithms=["HS256"],
                leeway=datetime.timedelta(seconds=60 * 60),
            )
            user_id = data["user_id"]
        except jwt.ExpiredSignatureError:
            return jsonify({"message": "Token has expired!"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"message": "Token is invalid!"}), 401

        # Pass the user_id to the endpoint
        return await f(user_id=user_id, *args, **kwargs)

    return decorated_function


@app.route("/googleTest")
async def googleTest():
    return 'Welcome to the Quart App! <br><a href="/register">Register</a> <br><a href="/login">Login</a> <br><a href="/register/google">Register with Google</a>'


@app.route("/login", methods=["POST"])
@validate_request(RegisterForm)
@brute_force_protection()
async def login(data: RegisterForm) -> Dict[str, Any]:
    return await loginUser(data)


@app.route("/register/google")
async def register_google() -> redirect:
    authorization_url, state = flow.authorization_url()
    return jsonify({"authorization_url": authorization_url})


@app.route("/callback")
async def authorize():
    return await authorize_user(request)


@app.route("/registration", methods=["POST"])
@validate_request(RegisterForm)
async def registration(data: RegisterForm):
    return await addUser(data)


@app.route("/registration-tg", methods=["POST"])
@validate_request(RegisterFormTelegram)
async def registration_tg(data: RegisterFormTelegram):
    data = asdict(data)
    return await addTgUser(data)


@app.route("/generate_names", methods=["POST"])
@validate_request(GenerationLogin)
async def generate_names(data: GenerationLogin):
    data = asdict(data)
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


@app.route("/upload-photo", methods=["POST"])
@authorized
async def upload_photo(user_id):
    photo = await request.files["photo"]
    return await addUserPhoto(user_id, photo)


@app.route("/refresh-token", methods=["POST"])
@validate_request(Token)
async def refresh_token(data: Token):
    return await refresh_user_token(data)


@app.route("/users", methods=["GET"])
@authorized
async def get_users() -> List[Dict[str, Any]]:
    return asyncio.run(get_all_users())


@app.route("/delete", methods=["DELETE"])
@authorized
async def delete(user_id):
    return await delete_account(user_id)


@app.route("/start-messaging", methods=["GET", "POST"])
@authorized
async def start_chat(user_id):
    return await start_messaging(request, user_id)


@app.route("/select_user", methods=["GET", "POST"])
@validate_request(RegisterForm)
async def select_user_to_chat():
    return await select_user(request)


@app.route("/count_posts", methods=["GET"])
@authorized
async def count_posts(user_id):
    # Получаем параметры запроса

    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    return await postsOfTime(user_id, start_time, end_time)


@app.route("/chat", methods=["GET"])
@authorized
async def talk(user_id):
    return await chat(user_id)


@app.route("/messages", methods=["GET"])
@authorized
async def get_messages(user_id):
    return await messages(request, user_id)


@app.websocket("/ws")
@authorized
async def ws(user_id):
    return await websocket(user_id)


# @app.route('/predict/<int:tg_user_id>', methods=['GET'])
# @authorized
# async def predict() -> any:
#     return asyncio.run(predict_post())

@app.route("/schemas", methods=["POST"])
@validate_request(Schemas)
async def get_schemas():
    return 'ок'

@app.route("/posts", methods=["POST"])
@authorized
@validate_request(Posts)
async def prediction(user_id, data: Posts):
    data = asdict(data)
    return await predict_posts(user_id, data)

@app.route("/show-keyphrases", methods=["POST"])
@authorized
@validate_request(KeyPhrases)
@rate_limit()
async def show_keyphrases(user_id, data: KeyPhrases):
    data = asdict(data)
    return await showKeyphrases(user_id, data)

@app.route("/predict/<int:tg_user_id>", methods=["GET"])
async def predict(tg_user_id) -> any:
    return await predict_post_tg(tg_user_id)


@app.route("/media/<path:filename>", methods=["GET"])
async def get_media(filename):
    try:
        return await send_file(filename)
    except FileNotFoundError:
        return jsonify({"message": "File not found"}), 404


@app.route("/upload_file_server", methods=["POST"])
async def upload_file():
    return await addFileServer(request)


@app.route("/quiz-word", methods=["GET"])
@authorized
async def get_random_word(user_id):
    return await randomWord(user_id)


@app.route("/add-reaction", methods=["POST"])
@authorized
@validate_request(UserReaction)
async def add_reaction(user_id, data: UserReaction):
    data = asdict(data)
    return await addReaction(user_id, data)


@app.route("/list-reaction", methods=["GET"])
@authorized
async def list_reaction(user_id):
    return await listReaction(user_id)


# @app.route('/check-answer', methods=['POST'])
# async def check_answer():
#     try:
#         # Получаем данные от пользователя
#         data = await request.get_json()
#         user_word = data.get('word')
#         user_answer = data.get('answer')
#         user_id = data.get('user_id')

#         # Проверяем данные
#         if not user_word or not user_answer or not user_id:
#             return jsonify({"message": "Word, answer, and user ID are required."}), 400

#         # Шаг 5: Найти слово в таблице keywords
#         word_doc = await r.table('keywords').filter({'word': user_word}).run(connection)
#         word = await word_doc.next()

#         if not word:
#             return jsonify({"message": "Word not found in the database."}), 404

#         # Проверка ответа пользователя
#         correct_answer = word['correct_answer']
#         if user_answer.lower() == correct_answer.lower():
#             # Увеличиваем баллы и уровни
#             await update_user_score(user_id, 1)  # Добавляем 1 балл
#             return jsonify({"message": "Correct!", "points": 1, "level": await get_user_level(user_id)})
#         else:
#             # Возвращаем правильный ответ и перевод, если не угадали
#             response = {
#                 "message": "Incorrect.",
#                 "correct_answer": correct_answer,
#                 "translation": word['translation']}
#             return jsonify(response)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


# async def update_user_score(user_id, points):
#     # Обновление баллов пользователя в базе данных
#     await r.table('users').get(user_id).update({"score": r.row["score"] + points}).run(connection)


# async def get_user_level(user_id):
#     # Получение текущего уровня пользователя
#     user = await r.table('users').get(user_id).run(connection)
#     score = user.get('score', 0)

#     # Определение уровня на основе баллов
#     if score < 10:
#         return "Beginner"
#     elif score < 20:
#         return "Intermediate"
#     else:
#         return "Advanced"


@app.route("/add-favorite", methods=["POST"])
@authorized
@validate_request(Favorites)
async def add_favorite(user_id, data: Favorites):
    data = asdict(data)
    return await addFavorite(user_id, data)


@app.route("/show-favorites", methods=["GET"])
@authorized
async def show_favorites(user_id):
    return await showFavorites(user_id)


@app.route("/top-posts", methods=["GET"])
@authorized
async def top_posts(user_id):
    limit = request.args.get("limit", default=10, type=int)
    start_time = request.args.get("start_date")
    end_time = request.args.get("end_date")
    return await topPosts(user_id, start_time, end_time, limit)


@app.route("/remove-favorite/<string:post_id>", methods=["DELETE"])
@authorized
async def remove_favorite(user_id, post_id):
    return await removeFavorite(user_id, post_id)


@app.route(
    "/remove-reaction/<string:post_id>/<string:reaction_type>", methods=["DELETE"]
)
@authorized
async def remove_reaction(user_id, post_id, reaction_type):
    return await removeReaction(user_id, post_id, reaction_type)


if __name__ == "__main__":
    app.run(host=Config.app.host, port=8000, debug=True)
