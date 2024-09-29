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
conn = rdb.connect(host="localhost", port=28015)

if not rdb.db_list().contains("meetingsDb").run(conn):
    rdb.db_create("meetingsDb").run(conn)

if not rdb.db("meetingsDb").table_list().contains("keywords").run(conn):
    rdb.db("meetingsDb").table_create("keywords").run(conn)

if not rdb.db("meetingsDb").table_list().contains("keyphrases").run(conn):
    rdb.db("meetingsDb").table_create("keyphrases").run(conn)

data_dir = (
    Path.home() / "projects" / "redengine" / "redengine" / "dataLoads" / "dataSets/"
)
filename = Path("polaroids.ai.data_fresh.json")
data_dir = Path(data_dir)
with open(data_dir / filename) as json_file:
    data = json.load(json_file)
    for p in data["posts"]:
        for word in p["keywords"]:
            repeat_words = (
                rdb.db("meetingsDb")
                .table("keywords")
                .filter({"word": word["keyword_or_phrase"]})
                .nth(0)
                .default(None)
                .run(conn)
            )

            if not repeat_words:
                rdb.db("meetingsDb").table("keywords").insert(
                    {
                        "word": word["keyword_or_phrase"],
                        "explanation": word["explanation"],
                    }
                ).run(conn)
        # for phrase in p["keyphrases"]:
        #     repeat_phrase = rdb.db('meetingsDb').table('keyphrases').filter({'phrase': phrase["keyword_or_phrase"]}).nth(0).default(None).run(conn)

        #     if not repeat_phrase:
        #             rdb.db('meetingsDb').table('keyphrases').insert({'phrase': phrase["keyword_or_phrase"], 'explanation': phrase['explanation']}).run(conn)
