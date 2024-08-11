import ast
from pathlib import Path

import rethinkdb as r
import uuid
import ast
from pathlib import Path
from typing import List, Union

import json

# data_dir = Path.home() / "Projects" / "BotBot" / "polaroids" / "polaroids" / "dataLoads" / "dataSets"
# filename = Path("/oikSmall.csv")

rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)

if not rdb.db_list().contains('meetingsDb').run(conn):
    rdb.db_create('meetingsDb').run(conn)

if not rdb.db('meetingsDb').table_list().contains('posts').run(conn):
    rdb.db('meetingsDb').table_create('posts').run(conn)

data_dir = Path.home() / "projects" / "redengine"/"redengine"/"dataLoads"/"dataSets/" 
filename = Path("polaroids.ai.data.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
    for p in data["posts"]:
        if "title" not in p:
            p["title"]=None
        if "author" not in p:
            p["author"] = None
        id = uuid.uuid4()

        book = rdb.db('meetingsDb').table('books').filter({'label': p["title"]}).nth(0).default(None).run(conn)
        author = rdb.db('meetingsDb').table('authors').filter({'name':p["author"]}).nth(0).default(None).run(conn)

        img_path= None
        if p["has_image"]:
            img_path = p["img_path"]
        if book and author:
            rdb.db('meetingsDb').table('posts').insert({'context': p["content"], 'book_id': str(book["id"]),'author_id': str(author["id"]),
                                                          'has_image':p["has_image"], 'img_path':img_path}).run(conn)