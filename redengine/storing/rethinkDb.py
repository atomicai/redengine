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

    async def addUser(self, email, hashed_password, refresh_token):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            await rdb.db(self.db).table('users').insert(
            {'email': email, 'password': hashed_password, 'refresh_token': refresh_token, 'active': True, 'created_at': datetime.utcnow()}).run(connection)      

    async def updateRefreshToken(self, refresh_token):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').update({'refresh_token': refresh_token}).run(connection)

    async def deleteAccount(self, id):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').filter({'id': id}).update(
            {'active': False }).run(connection)  

    async def usersList(self):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
            return await rdb.db(self.db).table('users').run(connection)
  

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

    async def messages(self, user_id, chat_user_id, page, limit):
        async with await rdb.connect(host=self.host, port=self.port) as connection:
          return await rdb.db(Config.db.database).table('messages').filter(
        (r.row['sender_id'] == user_id) & (r.row['receiver_id'] == chat_user_id) |
        (r.row['sender_id'] == chat_user_id) & (r.row['receiver_id'] == user_id)
    ).order_by(r.desc('created_at')).slice((page - 1) * limit, page * limit).run(connection)
           
#RethinkDb = IReDocStore()


__all__ = ["IReDocStore"]