import ast
from pathlib import Path

import rethinkdb as r
import uuid
import ast
from pathlib import Path
from typing import List, Union

import json



rdb = r.RethinkDB()
conn = rdb.connect(host='localhost', port=28015)


if not rdb.db_list().contains('meetingsDb').run(conn):
    rdb.db_create('meetingsDb').run(conn)

if not rdb.db('meetingsDb').table_list().contains('authors').run(conn):
    rdb.db('meetingsDb').table_create('authors').run(conn)


data_dir = Path.home() / "projects" / "redengine"/"redengine"/"dataLoads"/"dataSets/" 
filename = Path("polaroids.ai.data.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
    for p in data["posts"]:
        if "author" not in p:
            p["author"]=None



        repeat = list(rdb.db('meetingsDb').table('authors').filter({'name':p["author"]}).run(conn))

        if not repeat:

            id = uuid.uuid4()
            if "speaker" not in p:
                rdb.db('meetingsDb').table('authors').insert({'name': p["author"],'id': str(id),'is_speaker':False}).run(conn)
            else:
                rdb.db('meetingsDb').table('authors').insert({'name': p["author"],'id': str(id),'is_speaker':True}).run(conn)