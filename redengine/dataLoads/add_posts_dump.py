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
filename = Path("polaroids.ai.data_fresh.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
   
    for p in data["posts"]:
           
        if p["type"] == "movie":              
            if "keyphrases" not in p:
                p["keyphrases"] = []
            if "keywords" not in p:
                p["keywords"] = []
            speaker = rdb.db('meetingsDb').table('movie_speakers').filter({'name_speaker': p["speaker"]}).nth(0).default(None).run(conn)
            movie = rdb.db('meetingsDb').table('movies').filter({'title': p["title"]}).nth(0).default(None).run(conn)
  
            keywords = rdb.db('meetingsDb').table('keywords').filter(lambda keyword: rdb.expr(p["keywords"]).contains(keyword['word'])).run(conn)

            keywords_ids = []
            for word in keywords:
                keywords_ids.append(word["id"])

            keyphrases = rdb.db('meetingsDb').table('keyphrases').filter(lambda keyphrase: rdb.expr(p["keyphrases"]).contains(keyphrase['phrase'])).run(conn)
            
            keyphrases_ids = []
            for phrase in keyphrases:
                keyphrases_ids.append(phrase["id"])
            
            rdb.db('meetingsDb').table('posts').insert({'context': p["content"],'translation':p["translation"],'type': p['type'],'movie_id': str(movie["id"]),'speaker_id': str(speaker["id"]), 'has_image':p["has_image"], 'img_path':p["img_path"],'keywords_ids': keywords_ids,'keyphrases_ids': keyphrases_ids}).run(conn)
        elif p["type"] == "book":
            if "keyphrases" not in p:
                p["keyphrases"] = []
            if "keywords" not in p:
                p["keywords"] = []                                                 
            if "title" not in p:
                p["title"]=None
            if "author" not in p:
                p["author"] = None
            
            words = [item["keyword_or_phrase"] for item in p["keywords"]]
            phrases = [item["keyword_or_phrase"] for item in p["keyphrases"]]
            keywords = rdb.db('meetingsDb').table('keywords').filter(lambda keyword: rdb.expr(words).contains(keyword['word'])).run(conn)

            keywords_ids = []
            for word in keywords:
                keywords_ids.append(word["id"])

            keyphrases = rdb.db('meetingsDb').table('keyphrases').filter(lambda keyphrase: rdb.expr(phrases).contains(keyphrase['phrase'])).run(conn)

            keyphrases_ids = []
            for phrase in keyphrases:
                keyphrases_ids.append(phrase["id"])

            book = rdb.db('meetingsDb').table('books').filter({'label': p["title"]}).nth(0).default(None).run(conn)
            author = rdb.db('meetingsDb').table('authors').filter({'name':p["author"]}).nth(0).default(None).run(conn)
            img_path= None
            if p["has_image"]:
                img_path = p["img_path"]
            if book and author:
                rdb.db('meetingsDb').table('posts').insert({'context': p["content"], 'book_id': str(book["id"]),'author_id': str(author["id"]),
                                                          'has_image':p["has_image"],'type': p['type'], 'img_path':img_path,'keywords_ids': keywords_ids,'translation':p["translation"],'keyphrases_ids': keyphrases_ids}).run(conn)        