import asyncio
import random
import aioredis
from queues.redis.redisQueue import (RedisQueue)
from werkzeug.exceptions import HTTPException
import os

import jwt
from datetime import datetime, timedelta
from quart import Quart, redirect, url_for, session, request, jsonify, g
from authlib.integrations.starlette_client import OAuth
import rethinkdb as r
import os
import pathlib
from pathlib import Path
import yaml
import requests
from flask import Flask, session, abort, redirect, request
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from pip._vendor import cachecontrol
import google.auth.transport.requests
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Dict, Any, Optional, Tuple, List

with open(str(Path(os.getcwd()).parent / "config.yaml"), 'r') as file:
    config = yaml.safe_load(file)

rdb = r.RethinkDB()
rdb.set_loop_type('asyncio')
JWT_SECRET_KEY = os.environ.get("SECRET_KEY")


class CustomException(HTTPException):
    code = 500
    description = 'Почта уже занята'


flow = Flow.from_client_config(
    client_config=config,
    scopes=["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email",
            "openid"],
    redirect_uri="http://127.0.0.1:5000/callback"
)


async def predict(id):
    red = aioredis.from_url("redis://localhost")
    pubsub = red.pubsub()

    await pubsub.psubscribe(f"channel:{id}_recieve")
    await RedisQueue(id).subscribe_queue()
    post = await RedisQueue(id).reader(pubsub)
    return post


async def generate_tokens(user_id):
    access_token = jwt.encode({'user_id': str(user_id), 'exp': datetime.utcnow() + timedelta(minutes=15)},
                              str(JWT_SECRET_KEY), algorithm='HS256')
    refresh_token = jwt.encode({'user_id': str(user_id), 'exp': datetime.utcnow() + timedelta(days=7)},
                               str(JWT_SECRET_KEY), algorithm='HS256')
    return access_token, refresh_token


async def verify_token(token: str) -> Optional[str]:
    try:
        decoded = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
        return decoded['user_id']
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


async def addUser(user):
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        hashed_password = generate_password_hash(user.password)
        existing = await rdb.db('meetingsBook').table('users').filter({'email': user.email}).nth(0).default(None).run(
            connection)
        if existing:
            raise CustomException()

        access_token, refresh_token = await generate_tokens(user.email)

        await rdb.db('meetingsBook').table('users').insert(
            {'email': user.email, 'password': hashed_password, 'refresh_token': refresh_token,'active': True}).run(connection)

        return {'access_token': access_token, 'refresh_token': refresh_token}


async def loginUser(user):
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        person = await rdb.db('meetingsBook').table('users').filter({'email': user.email}).nth(0).default(None).run(
            connection)
    if person['password'] and user and check_password_hash(person['password'], user.password):
        access_token, refresh_token = generate_tokens(user.email)
        await rdb.db('meetingsBook').table('users').update({'refresh_token': refresh_token}).run(
            connection)
        return {'access_token': access_token, 'refresh_token': refresh_token}

    return {'message': 'Invalid credentials'}, 401


async def predict_post():
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        book_ids = []
        posts = []
        books_info = rdb.db('meetingsBook').table('books').pluck("id").run(connection)
        for book_id in books_info:
            book_ids.append(book_id["id"])
        random_book_id = random.choice(book_ids)

        posts_info = list(rdb.db('meetingsBook').table('posts').filter({'book_id': random_book_id}).run(connection))
        for post in posts_info:
            posts.append(post)
        random_post = random.choice(posts)
        [author_name] = list(
            rdb.db('meetingsBook').table('authors').filter({'id': random_post['author_id']}).run(connection))
        [book_name] = list(rdb.db('meetingsBook').table('books').filter({'id': random_post['book_id']}).run(connection))
    return {'id': random_post['id'], "author_name": author_name['name'], "book_name": book_name['label'],
            'post': random_post['context'], "score": 20}


async def delete_account(id) -> any:
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        await rdb.db('meetingsBook').table('users').filter({'id': id}).update(
            {'active': False }).run(connection)


async def get_all_users() -> List[Dict[str, Any]]:
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        users_cursor = await rdb.table('users').run(connection)
        users = [user async for user in users_cursor]
        return users


async def authorize_user(request):
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        flow = Flow.from_client_config(
            client_config=config,
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
            id_token=credentials._id_token,
            request=token_request,
            audience=config["web"]["client_id"],
            clock_skew_in_seconds=5,
        )

        existing_user =await rdb.db('meetingsBook').table('users').filter({'email': str(user['email'])}).nth(0).default(None).run(
            connection)
        access_token, refresh_token = await generate_tokens(str(user['email']))
        if not existing_user:
            await rdb.db('meetingsBook').table('users').insert(
                {'email': user['email'], 'name': user['name'], 'refresh_token': refresh_token,'active':True }).run(
                connection)

        return {'access_token': access_token, 'refresh_token': refresh_token}


async def refresh_user_token(data):
    async with await rdb.connect(host=config["db"]["host"], port=config["db"]["port"]) as connection:
        refresh_token = data.refresh_token
        try:
            decoded = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=['HS256'])
            user_id = decoded['user_id']
            user = await rdb.db('meetingsBook').table('users').filter({'email': user_id}).run(connection)

            if user and user['refresh_token'] == refresh_token:
                access_token, new_refresh_token = generate_tokens(user_id)
                await rdb.db('meetingsBook').table('users').filter({'email': user_id}).update(
                    {'refresh_token': new_refresh_token}).run(connection)
            return {'access_token': access_token, 'refresh_token': new_refresh_token}
        except jwt.ExpiredSignatureError:
            return {'message': 'Refresh token expired'}, 401
        except jwt.InvalidTokenError:
            return {'message': 'Invalid token'}, 401
