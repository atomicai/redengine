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
data_dir = Path.home() / "projects" / "redengine"/"redengine"/"dataLoads"/"dataSets/" 
filename = Path("polaroids.ai.data.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
    k=0
    for p in data["posts"]:
        k=k+1
        print(k)
        print(p['author'])



# if not rdb.db_list().contains('meetingsBook').run(conn):
#     rdb.db_create('meetingsBook').run(conn)
#
# if not rdb.db('meetingsBook').table_list().contains('posts').run(conn):
#     rdb.db('meetingsBook').table_create('posts',primary_key='idx').run(conn)
#
# for row in arr:
#     id = uuid.uuid4()
#     rdb.db('meetingsBook').table('posts').insert({'context': row["context"], 'book_id': row["book_id"],'author_id': row["author_id"],'id': str(id)}).run(conn)
#
# __all__ = ["get_data"]