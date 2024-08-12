from pathlib import Path
from datetime import datetime
import abc
from redengine.etc.pattern import singleton
import rethinkdb as r
import uuid
import os
import yaml
from dotmap import DotMap
from redengine.configuring import Config
from redengine.tdk.lib import transliterate

rdb = r.RethinkDB()
rdb.set_loop_type('asyncio')

class IDBDocStore(abc.ABC):
    @abc.abstractmethod
    async def add_event(self, query: str, response):
        pass

@singleton
class IReDocStore(IDBDocStore):
    def __init__(self, host= Config.db.host, port = Config.db.port, db= Config.db.database):
        self.host = host
        self.port = port
        self.db = db
     
    async def add_event(self, query, response):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('events').insert(
                {'query': query,'response': response}).run(connection)

    async def personByEmail(self, email):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').filter({'email': email}).nth(0).default(None).run(
            connection)

    async def addUserByEmail(self, email, hashed_password, refresh_token):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').insert(
            {'email': email, 'password': hashed_password, 'login': email, 'refresh_token': refresh_token, 'active': True, 'created_at': datetime.now(rdb.make_timezone('00:00'))}).run(connection)      

    async def addUser(self,user_id, login, hashed_password, refresh_token):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').insert(
            {'user_id': user_id,'login': login, 'password': hashed_password, 'refresh_token': refresh_token, 'active': True, 'created_at': datetime.now(rdb.make_timezone('00:00'))}).run(connection)      

    async def updateRefreshToken(self, refresh_token):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            # table_info=await rdb.db(self.db).table('users').info().run(connection)
            # print(table_info)
            return await rdb.db(self.db).table('users').update({'refresh_token': refresh_token}).run(connection)

    async def deleteAccount(self, id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').filter({'id': id}).update(
            {'active': False }).run(connection)  

    async def usersList(self):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').run(connection)
  
    async def personByTgUserId(self, tg_user_id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').filter({'tg_user_id': tg_user_id}).nth(0).default(None).run(
            connection)

    async def telegramRegistration(self, tg_user_id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').insert(
            {'tg_user_id': tg_user_id, 'active': True, 'created_at': datetime.utcnow().isoformat()}).run(connection)     

    async def telegramUserAddPhoto(self, user_photo):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').filter({'tg_user_id': user_photo["tg_user_id"]}).update(user_photo).run(connection)  

    async def userAddPhoto(self, user_id, file_path):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').filter({'user_id': user_id}).update({'file_path': file_path}).run(connection)  

    async def telegramUserAddInfo(self, user_info):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').filter({'tg_user_id': user_info["tg_user_id"]}).update(user_info).run(connection)   

    async def userAddInfo(self, user_id, user_info):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').filter({'user_id': user_id}).update(user_info).run(connection)   

    async def sendReaction(self, user_reaction):
        (rdb.db('meetingsBook').table('events_' + transliterate(user_reaction.book_name.replace(" ", "_")))
        .insert(user_reaction).run(
            conn))

    async def cursorChat(self, user_id, chat_user_id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(Config.db.database).table('messages').filter(
            (rdb.row['sender_id'] == user_id) & (rdb.row['receiver_id'] == chat_user_id) |
            (rdb.row['sender_id'] == chat_user_id) & (rdb.row['receiver_id'] == user_id)
        ).changes().run(connection)       

    async def addMessage(self, user_id, chat_user_id,message):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(Config.db.database).table('messages').insert({
                'sender_id': user_id,
                'receiver_id': chat_user_id,
                'message': message,
                'created_at': datetime.utcnow()
            }).run(connection)  

    async def personById(self, user_id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return await rdb.db(Config.db.database).table('users').get(user_id).run(connection)  

    async def personByLogin(self, nickname):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return await rdb.db(Config.db.database).table('users').filter({'login': nickname}).nth(0).default(None).run(connection)  
        
    async def addFavorites(self, user_id, post_id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return await rdb.db(Config.db.database).table('favorites_posts').insert({'user_id': user_id,'post_id': post_id}).run(connection)  
        
    async def showFavorites(self, user_id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return  await rdb.db(Config.db.database).table('favorites_posts').filter({'user_id': user_id}).map(lambda doc: doc['post_id']).run(connection)
        
    async def PostById(self, post_ids):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return await rdb.db(Config.db.database).table('posts').filter(lambda post: rdb.expr(post_ids).contains(post['id'])).eq_join('book_id', rdb.db(Config.db.database).table('books')).zip().eq_join('author_id', rdb.db(Config.db.database).table('authors')).zip().run(connection)  

    async def messages(self, user_id, chat_user_id, page, limit):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return await rdb.db(Config.db.database).table('messages').filter(
        (r.row['sender_id'] == user_id) & (r.row['receiver_id'] == chat_user_id) |
        (r.row['sender_id'] == chat_user_id) & (r.row['receiver_id'] == user_id)
    ).order_by(r.desc('created_at')).slice((page - 1) * limit, page * limit).run(connection)
           
#RethinkDb = IReDocStore()


__all__ = ["IReDocStore"]