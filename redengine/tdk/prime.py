import asyncio
import random
#import aioredis
# from queues.redis.redisQueue import (RedisQueue)
from werkzeug.exceptions import HTTPException
import jwt
from datetime import datetime, timedelta
from quart import Quart, redirect, url_for, session, request, jsonify, g,render_template,websocket as wsd,send_file
from authlib.integrations.starlette_client import OAuth
import rethinkdb as r
import os
import pathlib
from pathlib import Path
import yaml
import uuid
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
import base64
import aiofiles
#import quart_swagger_ui as qswagger
import pytz


rdb = r.RethinkDB()
rdb.set_loop_type('asyncio')
JWT_SECRET_KEY = os.environ.get("SECRET_KEY")


RethinkDb = IReDocStore()

with open(str(Path(os.getcwd()) / "config.yaml")) as fp:
    flowConfig = yaml.safe_load(fp)

class CustomException(HTTPException):
    code = 500
    description = 'Почта уже занята'


class CustomLoginException(HTTPException):
    code = 500
    description = 'Логин уже занят'

flow = Flow.from_client_config(
    client_config=flowConfig,
    scopes=["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email",
            "openid"],
    redirect_uri="https://polaroids.ngrok.app/callback"
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
    access_token = jwt.encode({'user_id': str(user_id), 'exp': datetime.utcnow() + timedelta(minutes=60)},
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

async def generateName(data):
    data = await request.get_json()
    nickname = data['nickname']
    
    generated_names = set()
    while len(generated_names) < 4:
        name = f"{nickname}_{namegenerator.gen()}"
        busy_name = await RethinkDb.getLogin(nickname)
        if not busy_name:
            generated_names.add(name)

    return jsonify({'generated_names': list(generated_names)})

async def messages(request,user_id):
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 50))
    chat_user_id = session.get('chat_user_id')
    
    if not user_id or not chat_user_id:
        return jsonify([])

    messages = await RethinkDb.messages(user_id, chat_user_id, page, limit)

    return jsonify(messages)

async def removeFavorite(user_id,data):
        # Получаем данные из запроса
    data = await request.get_json()
    post_id = data.get('post_id')
    result = await RethinkDb.removeFavorite(user_id, post_id)
    
    if result['deleted'] > 0:
        return jsonify({"message": "Post removed from favorites"}), 200
    else:
        return jsonify({"message": "No matching favorite post found"}), 404
    
async def start_messaging(request, user_id):
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
        # existing = await RethinkDb.personByEmail(user.email)
        # # проверить ,есть ли подобные имена, если нет, то сохраняем, если есть , то выдаем предложенные 4 имени 
        # if existing:
        #     raise CustomException()
        existing = await RethinkDb.personByLogin(user.login)
        # проверить ,есть ли подобные имена, если нет, то сохраняем, если есть , то выдаем предложенные 4 имени 
        if existing:
            raise CustomLoginException()
        
        user_id = uuid.uuid4()
        access_token, refresh_token = await generate_tokens(str(user_id))
        user = await RethinkDb.addUser(str(user_id),user.login,hashed_password,refresh_token)
        return {'access_token': access_token, 'refresh_token': refresh_token}

async def randomWord(user_id):

        user_events = await RethinkDb.userEvents(user_id)
        post_ids = [event['post_id'] for event in user_events]

        if not post_ids:
            return jsonify({"message": "No posts found for the user in the last month."})

        posts = await RethinkDb.keywordsByPostIds(post_ids)
        
        all_word_ids = set()
        for post in posts:
            all_word_ids.update(post.get('keywords', []))

        if not all_word_ids:
            return jsonify({"message": "No words found in the posts for the user."})

        random_word_id = random.choice(list(all_word_ids))
        word_doc = await r.table('keywords').get(random_word_id).run(connection)

        if not word_doc:
            return jsonify({"message": "Word not found in the database."}), 404

        # Возвращаем слово и 3 объяснения
        response = {
            "word": word_doc['word'],
            "explanations": word_doc['explanations']
        }

        return jsonify(response)


async def addTgUser(data):
        existing = await RethinkDb.personByTgUserId(data["tg_user_id"])
        if not existing:
             await RethinkDb.telegramRegistration(data["tg_user_id"])
        return 'ok'

async def save_photo(photo, user_id):
    user_folder = os.path.join(Config.folder.photo, str(user_id))

    # Создаем директорию для пользователя, если она не существует
    if not os.path.exists(user_folder):
        os.makedirs(user_folder)

    # Путь для сохранения файла
    file_path = os.path.join(user_folder, photo.filename)

    # Асинхронное сохранение файла
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await photo.read()
        await out_file.write(content)

    return file_path
        
async def addTelegramUserPhoto(data):
        await RethinkDb.telegramUserAddPhoto(data)  
        return 'ok'   

async def addUserPhoto(user_id, photo):
        file_path = save_photo(photo, user_id)
        await RethinkDb.userAddPhoto(user_id, file_path)  
        return 'ok'


   # Swagger UI настройки
# qswagger.swagger_ui(app, swagger_url="/swagger", api_url="/openapi.json")

async def postsOfTime(user_id, start_time, end_time):
        start_time = datetime.fromisoformat(start_time).replace(tzinfo=pytz.utc)
        end_time = datetime.fromisoformat(end_time).replace(tzinfo=pytz.utc)
        count = await RethinkDb.countPostsOfTime(user_id, start_time, end_time)  
        return  {'count': count}

async def addTgUserInfo(data):
    await RethinkDb.telegramUserAddInfo(data)  
    return 'ok'     

async def addUserInfo(user_id, data):
    await RethinkDb.userAddInfo(user_id, data)  
    return 'ok' 

async def addTgUserReaction(data):
    userReaction = data.dict(exclude_unset=True)  
    await RethinkDb.sendReaction(userReaction)  

async def addUserReaction(user_id, data):
    await RethinkDb.sendReactionByUser(user_id,data)  
    return 'ok' 

async def addReaction(user_id, data):
    await RethinkDb.sendReaction(user_id,data)  
    return 'ok' 
# data.tg_user_id, data.username, data.age, data.describe

async def loginUser(user):
        person = None
        if user.email:
            person = await RethinkDb.personByEmail(user.email)
        else:
            person = await RethinkDb.personByLogin(user.login)
        if person['password'] and person and check_password_hash(person['password'], user.password):
            access_token, refresh_token = await generate_tokens(person["user_id"])
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
            redirect_uri="https://polaroids.ngrok.app/callback"
        )
        flow.fetch_token(authorization_response=request.url)
        print(flow)
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
            await RethinkDb.addUserByEmail(user["email"], None, refresh_token)

        return {'access_token': access_token, 'refresh_token': refresh_token}

async def refresh_user_token(data):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        refresh_token = data.refresh_token
        try:
            decoded = jwt.decode(refresh_token, Config.jwt_secret_key, algorithms=['HS256'])
            user_id = decoded['user_id']
            user = await RethinkDb.personById(user_id)

            if user and user['refresh_token'] == refresh_token:
                access_token, new_refresh_token = await generate_tokens(user_id)
                await rdb.db(Config.db.database).table('users').get({user_id}).update(
                    {'refresh_token': new_refresh_token}).run(connection)
            return {'access_token': access_token, 'refresh_token': new_refresh_token}
        except jwt.ExpiredSignatureError:
            return {'message': 'Refresh token expired'}, 401
        except jwt.InvalidTokenError:
            return {'message': 'Invalid token'}, 401
async def addFileServer(request):

    data_dir = Path.home() / "projects" / "redengine"/"redengine"/"dataLoads"/"dataSets/" 
    data_dir = Path(data_dir)
  
    files = await request.files
  
    # Проверка, что файл присутствует в запросе
    if 'file' not in files:
        return jsonify({"error": "No file part in the request"}), 400

    file = files['file']

    # Проверка, что файл выбран
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    # Проверка, что это JSON файл
    if not file.filename.endswith('.json'):
        return jsonify({"error": "File is not a JSON file"}), 400
 
    # Сохранение файла
    file_path = os.path.join(data_dir, file.filename)
    await file.save(file_path)

    return jsonify({"message": "File uploaded successfully", "file_path": file_path}), 200 

async def predict_post_tg(tg_user_id):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        print(tg_user_id)
        book_ids = []
        posts = []
        books_info = await rdb.db('meetingsDb').table('books').get('65cb28eb-55dd-43c8-baac-ba7d6408260b').run(connection)
        print(books_info)
        book_id = books_info["id"]
        posts_info = await rdb.db('meetingsDb').table('posts').filter({'book_id': book_id}).nth(0).default(None).run(connection)
        author_name = await rdb.db('meetingsDb').table('authors').filter({'id': posts_info['author_id']}).nth(0).default(None).run(connection)
        book_name = await rdb.db('meetingsDb').table('books').filter({'id': posts_info['book_id']}).nth(0).default(None).run(connection)
    return {'id': posts_info['id'], "author_name": author_name['name'], "book_name": book_name['label'],
            'post': posts_info['context'], "score": 20, "media_path": posts_info['img_path'], "media_type":"image"}

async def predict_posts1(user_id, data):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        random_posts = await rdb.db('meetingsDb').table('posts').sample(3).run(connection) 
        
        
        result = []
        for post in random_posts:
            post_info = await rdb.db('meetingsDb').table('posts').filter({"id":post['id']}).eq_join('speaker_id', rdb.db(Config.db.database).table('movie_speakers')).zip().eq_join('movie_id', rdb.db(Config.db.database).table('movies')).zip().nth(0).default(None).run(connection)

            keywords = await rdb.db(Config.db.database).table('keywords').filter(lambda keyword: rdb.expr(post_info['keywords_ids']).contains(keyword['id'])).run(connection)
   
            keyphrases = await rdb.db(Config.db.database).table('keyphrases').filter(lambda keyphrase: rdb.expr(post_info['keyphrases_ids']).contains(keyphrase['id'])).run(connection)

            post_keyphrases = []
            async for phrase in keyphrases:
                post_keyphrases.append(phrase['phrase'])
            post_keywords = []
            async for word in keywords:
                post_keywords.append(word['word'])
            # post = {'id': post['id'], "author_name": post['name'], "book_name": post['label'],
            # 'text': post['context'], "score": 20, "media_path": post['img_path'], "is_image": post["has_image"], "is_speaker": post["is_speaker"],"speaker_name": post_info["name_speaker"],"movie":post_info['title'] } 
            post = {'id': post['id'],
            'text': post['context'], "score": 20, "media_path": post['img_path'], "is_image": post["has_image"],"speaker_name": post_info["name_speaker"],"movie":post_info['title'],"keyphrases":post_keyphrases,"keywords":post_keywords } 
            result.append(post)
    
        return jsonify(result[:data["top"]])
        
async def predict_posts(user_id, data):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        posts = []
        result = []
        if not "post_ids" in data:
            random_posts = await rdb.db('meetingsDb').table('posts').sample(data['top']).pluck('id').map(lambda post: post['id']).run(connection)
            for prev_post in data["previous_posts"]:
                await addUserReaction(user_id,{"post_id": prev_post["post_id"],"reaction":prev_post["reaction"]})
            posts = await rdb.db(Config.db.database).table('posts').filter(lambda post: rdb.expr(random_posts).contains(post['id'])).run(connection)
        else:
            posts = await rdb.db(Config.db.database).table('posts').filter(lambda post: rdb.expr(data["post_ids"]).contains(post['id'])).run(connection)
        
        async for post in posts:
          
          reaction_counts = await rdb.db('meetingsDb').table('post_reaction').filter({'post_id' : post["id"]}).group('reaction_type').count().run(connection)
            
          if post['type'] == 'movie':
            post_info = await rdb.db('meetingsDb').table('posts').filter({"id":post['id']}).eq_join('speaker_id', rdb.db(Config.db.database).table('movie_speakers')).zip().eq_join('movie_id', rdb.db(Config.db.database).table('movies')).zip().nth(0).default(None).run(connection)
            keywords = await rdb.db(Config.db.database).table('keywords').filter(lambda keyword: rdb.expr(post_info['keywords_ids']).contains(keyword['id'])).run(connection)
            
            keyphrases = await rdb.db(Config.db.database).table('keyphrases').filter(lambda keyphrase: rdb.expr(post_info['keyphrases_ids']).contains(keyphrase['id'])).run(connection)
                    
            post_keyphrases = []
            async for phrase in keyphrases:

                start_index = post_info["translation"].find(phrase["phrase"])
                while start_index != -1:
                    end_index = start_index + len(phrase["phrase"])  

                    phrase["start_index"] = start_index
                    phrase["end_index"] = end_index
                    start_index = post_info["translation"].find(phrase["phrase"], start_index + 1)

                post_keyphrases.append(phrase)
            post_keywords = []
            async for word in keywords:
                post_keywords.append(word)

            post = {'id': post['id'],
            'text': post['context'], "score": 20,"reaction_counts":reaction_counts, "media_path": post['img_path'], "is_image": post["has_image"],"speaker_name": post_info["name_speaker"],"movie":post_info['title'],'type':post_info['type'],"keyphrases": post_keyphrases,"keywords": post_keywords } 
            result.append(post)

          elif post['type'] == 'book':
            post_info = await rdb.db('meetingsDb').table('posts').filter({"id":post['id']}).eq_join('book_id', rdb.db(Config.db.database).table('books')).zip().eq_join('author_id', rdb.db(Config.db.database).table('authors')).zip().nth(0).default(None).run(connection)

            keywords = await rdb.db(Config.db.database).table('keywords').filter(lambda keyword: rdb.expr(post_info['keywords_ids']).contains(keyword['id'])).run(connection)
            
            keyphrases = await rdb.db(Config.db.database).table('keyphrases').filter(lambda keyphrase: rdb.expr(post_info['keyphrases_ids']).contains(keyphrase['id'])).run(connection)
            
            post_keyphrases = []
            async for phrase in keyphrases:
                start_index = (post_info["translation"]).find(phrase["phrase"])
                while start_index != -1:
                    end_index = start_index + len(phrase["phrase"])
                    phrase["start_index"] = start_index
                    phrase["end_index"] = end_index
                    start_index = post_info["translation"].find(phrase["phrase"], start_index + 1)


                post_keyphrases.append(phrase)
            post_keywords = []

            async for keyword in keywords:
                start_index = post_info["translation"].find(keyword["word"])
        # Ищем все вхождения ключевого слова
                while start_index != -1:
                    end_index = start_index + len(keyword["word"])  

                    keyword["start_index"] = start_index
                    keyword["end_index"] = end_index
                    start_index = post_info["translation"].find(keyword["word"], start_index + 1)

                    post_keywords.append(keyword)

            post = {'id': post['id'],
            'text': post['context'], "score": 20, "media_path": post['img_path'],"reaction_counts":reaction_counts, "is_image": post["has_image"],"translation":post["translation"],"author_name": post_info['name'], "book_name": post_info['label'],'type':post_info['type'],"keyphrases": post_keyphrases,"keywords": post_keywords } 
            
            result.append(post)
        return jsonify(result[0:data["top"]])


async def addFavorite(user_id, data):
    if not user_id or not data:
        return jsonify({"error": "user_id and post_text are required"}), 400
    
    await RethinkDb.addFavorites(user_id, data['post_id'])

    return 'ok'

async def showFavorites(user_id):
    async with await rdb.connect(host=Config.db.host, port=Config.db.port) as connection:
        favorites = await RethinkDb.showFavorites(user_id)

        post_ids = []
        async for id in favorites:
            post_ids.append(id)
        posts = await rdb.db(Config.db.database).table('posts').filter(lambda post: rdb.expr(post_ids).contains(post['id'])).run(connection)
        result = []
        async for post in posts:
          if post['type'] == 'movie':
            post_info = await rdb.db('meetingsDb').table('posts').filter({"id":post['id']}).eq_join('speaker_id', rdb.db(Config.db.database).table('movie_speakers')).zip().eq_join('movie_id', rdb.db(Config.db.database).table('movies')).zip().nth(0).default(None).run(connection)
            keywords = await rdb.db(Config.db.database).table('keywords').filter(lambda keyword: rdb.expr(post_info['keywords_ids']).contains(keyword['id'])).run(connection)
            
            keyphrases = await rdb.db(Config.db.database).table('keyphrases').filter(lambda keyphrase: rdb.expr(post_info['keyphrases_ids']).contains(keyphrase['id'])).run(connection)
            
            post_keyphrases = []
            async for phrase in keyphrases:
                post_keyphrases.append(phrase)
            post_keywords = []
            async for word in keywords:
                post_keywords.append(word)

            post = {'id': post['id'],
            'text': post['context'], "score": 20, "media_path": post['img_path'], "is_image": post["has_image"],"speaker_name": post_info["name_speaker"],"movie":post_info['title'],'type':post_info['type'],"keyphrases": post_keyphrases,"keywords": post_keywords } 
            result.append(post)

          elif post['type'] == 'book':
            post_info = await rdb.db('meetingsDb').table('posts').filter({"id":post['id']}).eq_join('book_id', rdb.db(Config.db.database).table('books')).zip().eq_join('author_id', rdb.db(Config.db.database).table('authors')).zip().nth(0).default(None).run(connection)

            keywords = await rdb.db(Config.db.database).table('keywords').filter(lambda keyword: rdb.expr(post_info['keywords_ids']).contains(keyword['id'])).run(connection)
            
            keyphrases = await rdb.db(Config.db.database).table('keyphrases').filter(lambda keyphrase: rdb.expr(post_info['keyphrases_ids']).contains(keyphrase['id'])).run(connection)
            
            post_keyphrases = []
            async for phrase in keyphrases:
                post_keyphrases.append(phrase)
            post_keywords = []
            async for word in keywords:
                post_keywords.append(word)

            post = {'id': post['id'],
            'text': post['context'], "score": 20, "media_path": post['img_path'], "is_image": post["has_image"],"author_name": post_info['name'], "book_name": post_info['label'],'type':post_info['type'],"keyphrases": post_keyphrases,"keywords": post_keywords } 
            result.append(post)
    
    return jsonify(result)

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