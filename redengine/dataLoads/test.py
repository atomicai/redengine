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


post = (
    rdb.db("meetingsDb")
    .table("posts")
    .filter(
        {
            "translation": "Snape, who was sitting to the right of Dumbledore, didn't stand up when his name was mentioned, just lazily raised his hand in response to the applause from the Slytherin side of the table, but Harry was sure he saw a triumphant expression on that hateful face. - One good thing, - he said angrily, - by the end of the year we'll get rid of Snape. - What do you mean? - Ron didn't understand. - This position is cursed. Nobody has lasted more than a year on it... Quirrell died. I will personally keep my fingers crossed - maybe someone else will die... - Harry! - Hermione scolded in shock. - Maybe at the end of the year, he'll just go back to his magic potions, - Ron remarked sensibly. - Maybe this Slughorn won't want to stay for long. Grubb didn't want to. Dumbledore coughed. Not only Harry, Ron, and Hermione were distracted by conversations; the whole hall was discussing the stunning news that Snape had finally seen his cherished dream come true. As if not noticing the sensational information he had just given, Dumbledore said nothing more about staff shifts. Having waited until absolute silence set in, he began to speak again."
        }
    )
    .run(conn)
)
print(post)
