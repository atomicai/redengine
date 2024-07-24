import asyncio
import random
#import aioredis
# from queues.redis.redisQueue import (RedisQueue)
from werkzeug.exceptions import HTTPException
import jwt
from datetime import datetime, timedelta
from quart import Quart, redirect, url_for, session, request, jsonify, g,render_template,websocket as wsd
from authlib.integrations.starlette_client import OAuth
import rethinkdb as r
import os
import pathlib
from pathlib import Path
import yaml
import requests
from redengine.storing.rethinkDb import IReDocStore
from flask import Flask, session, abort, redirect, request
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from pip._vendor import cachecontrol
import google.auth.transport.requests
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Dict, Any, Optional, Tuple, List
from redengine.configuring import Config


rdb = r.RethinkDB()
rdb.set_loop_type('asyncio')
JWT_SECRET_KEY = os.environ.get("SECRET_KEY")


RethinkDb = IReDocStore()

with open(str(Path(os.getcwd()) / "config.yaml")) as fp:
    flowConfig = yaml.safe_load(fp)

class CustomException(HTTPException):
    code = 500
    description = 'Почта уже занята'


flow = Flow.from_client_config(
    client_config=flowConfig,
    scopes=["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email",
            "openid"],
    redirect_uri="http://127.0.0.1:5000/callback"
)


async def predict(id):
    # red = aioredis.from_url("redis://localhost")
    # pubsub = red.pubsub()

    # await pubsub.psubscribe(f"channel:{id}_recieve")
    # await RedisQueue(id).subscribe_queue()
    # post = await RedisQueue(id).reader(pubsub)
    # return post
    return 'ok'

async def websocket(user_id):
    user_id = user_id
    chat_user_id = session.get('chat_user_id')
    if not user_id or not chat_user_id:
        return

    async def send_messages():
        while True:
            cursor =await RethinkDb.cursorChat(user_id,chat_user_id)
        
        async for change in cursor:
            message = change['new_val']
            await wsd.send_json(message)
    
    async def receive_messages():
        while True:
            data = await wsd.receive_json()
            await RethinkDb.addMessage(user_id,chat_user_id,data["message"])
    
    await asyncio.gather(send_messages(), receive_messages())

async def generate_tokens(user_id):
    access_token = jwt.encode({'user_id': str(user_id), 'exp': datetime.utcnow() + timedelta(minutes=15)},
                              str(Config.jwt_secret_key), algorithm='HS256')
    refresh_token = jwt.encode({'user_id': str(user_id), 'exp': datetime.utcnow() + timedelta(days=7)},
                               str(Config.jwt_secret_key), algorithm='HS256')
    return access_token, refresh_token


async def verify_token(token: str) -> Optional[str]:
    try:
        decoded = jwt.decode(token, Config.jwt_secret_key, algorithms=['HS256'])
        return decoded['user_id']
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

async def chat(user_id):
    chat_user_id = session.get('chat_user_id')
    if user_id and chat_user_id:
        user = await RethinkDb.personById(user_id)
        chat_user = await RethinkDb.personById(chat_user_id)
        return await render_template('chat.html', user=user, chat_user=chat_user)
    return redirect(url_for('start_messaging'))

async def messages(request,user_id):
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 50))
    chat_user_id = session.get('chat_user_id')
    
    if not user_id or not chat_user_id:
        return jsonify([])

    messages = await RethinkDb.messages(user_id, chat_user_id, page, limit)

    return jsonify(messages)

async def start_messaging(request,user_id):
        if request.method == 'POST':
            return await render_template('index.html')

async def select_user(request):
        users = await RethinkDb.usersList()
        if request.method == 'POST':
            data = await request.form
            session['chat_user_id'] = data['user_id']
            return redirect(url_for('chat'))
        return await render_template('select_user.html', users=users)

async def addUser(user):
        
        hashed_password = generate_password_hash(user.password)
        existing = await RethinkDb.personByEmail(user.email)

        if existing:
            raise CustomException()

        access_token, refresh_token = await generate_tokens(user.email)
        await RethinkDb.addUser(user.email,hashed_password,refresh_token)

        return {'access_token': access_token, 'refresh_token': refresh_token}

async def addTgUser(data):
        existing = await RethinkDb.personByTgUserId(data["tg_user_id"])
        if not existing:
             await RethinkDb.telegramRegistration(data["tg_user_id"])
        return 'ok'

        
async def addTelegramUserPhoto(data):
        await RethinkDb.telegramUserAddPhoto(data)  
        return 'ok'   

async def addTgUserInfo(data):
    await RethinkDb.telegramUserAddInfo(data)  
    return 'ok'     

async def addTgUserReaction(data):
    userReaction = data.dict(exclude_unset=True)  
    await RethinkDb.sendReaction(userReaction)  
# data.tg_user_id, data.username, data.age, data.describe

async def loginUser(user):
        person = await RethinkDb.personByEmail(user.email)

        if person['password'] and user and check_password_hash(person['password'], user.password):
            access_token, refresh_token = await generate_tokens(user.email)
            
            await RethinkDb.updateRefreshToken(refresh_token)
            
            return {'access_token': access_token, 'refresh_token': refresh_token}

        return {'message': 'Invalid credentials'}, 401


async def delete_account(id) -> any:
        await RethinkDb.deleteAccount(id)

async def get_all_users() -> List[Dict[str, Any]]:
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        users_cursor = await rdb.table('users').run(connection)
        users = [user async for user in users_cursor]
        return users


async def authorize_user(request):
        flow = Flow.from_client_config(
            client_config=flowConfig,
            scopes=["https://www.googleapis.com/auth/userinfo.profile",
                    "https://www.googleapis.com/auth/userinfo.email",
                    "openid"],
            redirect_uri="http://127.0.0.1:5000/callback"
        )
        flow.fetch_token(authorization_response=request.url)

        credentials = flow.credentials
        request_session = requests.session()
        cached_session = cachecontrol.CacheControl(request_session)
        token_request = google.auth.transport.requests.Request(session=cached_session)

        user = id_token.verify_oauth2_token(
            id_token = credentials._id_token,
            request = token_request,
            audience = Config.web.client_id,
            clock_skew_in_seconds=5,
        )

        existing_user = await RethinkDb.personByEmail(user["email"])
        access_token, refresh_token = await generate_tokens(str(user['email']))
        if not existing_user:
            await RethinkDb.addUser(user["email"], None, refresh_token)

        return {'access_token': access_token, 'refresh_token': refresh_token}

async def refresh_user_token(data):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        refresh_token = data.refresh_token
        try:
            decoded = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=['HS256'])
            user_id = decoded['user_id']
            user = await RethinkDb.personByEmail(user_id)

            if user and user['refresh_token'] == refresh_token:
                access_token, new_refresh_token = await generate_tokens(user_id)
                await rdb.db('meetingsBook').table('users').filter({'email': user_id}).update(
                    {'refresh_token': new_refresh_token}).run(connection)
            return {'access_token': access_token, 'refresh_token': new_refresh_token}
        except jwt.ExpiredSignatureError:
            return {'message': 'Refresh token expired'}, 401
        except jwt.InvalidTokenError:
            return {'message': 'Invalid token'}, 401

async def predict_post(tg_user_id):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        print(tg_user_id)
        book_ids = []
        posts = []
        books_info = await rdb.db('meetingsDb').table('books').get('a1eec874-7d10-4e3d-bcfb-24712bfb0941').run(connection)
        print(books_info)
        book_id = books_info["id"]
        posts_info = await rdb.db('meetingsDb').table('posts').filter({'book_id': book_id}).nth(0).default(None).run(connection)
        author_name = await rdb.db('meetingsDb').table('authors').filter({'id': posts_info['author_id']}).nth(0).default(None).run(connection)
        book_name = await rdb.db('meetingsDb').table('books').filter({'id': posts_info['book_id']}).nth(0).default(None).run(connection)
    return {'id': posts_info['id'], "author_name": author_name['name'], "book_name": book_name['label'],
            'post': posts_info['context'], "score": 20, "media_path": posts_info['img_path'], "media_type":"image"}






    # async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
    #     book_ids = []
    #     posts = []
    #     books_info = rdb.db('meetingsBook').table('books').pluck("id").run(connection)
    #     for book_id in books_info:
    #         book_ids.append(book_id["id"])
    #     random_book_id = random.choice(book_ids)
    #     posts_info = list(rdb.db('meetingsBook').table('posts').filter({'book_id': random_book_id}).run(connection))
    #     for post in posts_info:
    #         posts.append(post)
    #     random_post = random.choice(posts)
    #     [author_name] = list(
    #         rdb.db('meetingsBook').table('authors').filter({'id': random_post['author_id']}).run(connection))
    #     [book_name] = list(rdb.db('meetingsBook').table('books').filter({'id': random_post['book_id']}).run(connection))
    # return {'id': random_post['id'], "author_name": author_name['name'], "book_name": book_name['label'],
    #         'post': random_post['context'], "score": 20}