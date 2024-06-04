
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

from werkzeug.security import generate_password_hash, check_password_hash
from typing import Dict, Any, Optional, Tuple, List


rdb = r.RethinkDB()
rdb.set_loop_type('asyncio')
JWT_SECRET_KEY = os.environ.get("SECRET_KEY")


class CustomException(HTTPException):
    code = 500
    description = 'Почта уже занята'


async def predict(id):
    red = aioredis.from_url("redis://localhost")
    pubsub = red.pubsub()

    await pubsub.psubscribe(f"channel:{id}_recieve")
    await RedisQueue(id).subscribe_queue()
    post = await RedisQueue(id).reader(pubsub)
    return post


async def generate_tokens(user_id: str) -> Tuple[str, str]:
    access_token = jwt.encode({'user_id': user_id, 'exp': datetime.utcnow() + timedelta(minutes=15)},
                              JWT_SECRET_KEY, algorithm='HS256')
    refresh_token = jwt.encode({'user_id': user_id, 'exp': datetime.utcnow() + timedelta(days=7)},
                               JWT_SECRET_KEY, algorithm='HS256')
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
    async with await rdb.connect(host='localhost', port=28015) as connection:
        hashed_password = generate_password_hash(user.password)
        existing = await rdb.db('meetingsBook').table('users').filter({'email': user.email}).nth(0).default(None).run(
            connection)
        if existing:
            raise CustomException()

        access_token, refresh_token = await generate_tokens(user.email)

        await rdb.db('meetingsBook').table('users').insert(
            {'email': user.email, 'password': hashed_password, 'refresh_token': refresh_token}).run(connection)

        return {'access_token': access_token, 'refresh_token': refresh_token}


async def loginUser(user):
    async with await rdb.connect(host='localhost', port=28015) as connection:
        person = await rdb.db('meetingsBook').table('users').filter({'email': user.email}).nth(0).default(None).run(
            connection)
    if user and check_password_hash(person['password'], user.password):
        access_token, refresh_token = generate_tokens(user.email)
        await rdb.db('meetingsBook').table('users').update({'refresh_token': refresh_token}).run(
            connection)
        return {'access_token': access_token, 'refresh_token': refresh_token}

    return {'message': 'Invalid credentials'}, 401


async def predict_post():
    async with await rdb.connect(host='localhost', port=28015) as connection:
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
    pass

async def get_all_users() -> List[Dict[str, Any]]:
    async with await rdb.connect(host='localhost', port=28015) as connection:
        users_cursor = await rdb.table('users').run(connection)
        users = [user async for user in users_cursor]
        return users


async def authorize_user() -> str:
    async with await rdb.connect(host='localhost', port=28015) as connection:
        token = await OAuth.google.authorize_access_token()
        user_info = await OAuth.google.parse_id_token(token)

        user_id: str = user_info['sub']
        email: str = user_info['email']
        name: str = user_info.get('name')

    existing_user = await rdb.db('meetingsBook').table('users').filter({'email': email}).nth(0).default(None).run(
        connection)

    if not existing_user:
        await rdb.db('meetingsBook').table('users').insert({'email': email, 'name': name, 'refresh_token': ''}).run(
            connection)

    return 'Registration successful. You can now log in with your Google account.'


async def refresh_user_token() -> Dict[str, Any]:
    async with await rdb.connect(host='localhost', port=28015) as connection:
        data = await request.json
        refresh_token: str = data['refresh_token']

        try:
            decoded: Dict[str, Any] = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=['HS256'])
            user_id: str = decoded['user_id']
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
