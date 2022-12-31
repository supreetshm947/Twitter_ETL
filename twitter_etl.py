import json
import couchdb
import tweepy
import pandas as pd
import credentials
from couchdb.mapping import Document, IntegerField, DateTimeField, TextField


class Tweet(Document):
    author_id = IntegerField
    created_at = DateTimeField
    tweet_id = IntegerField
    lang = TextField
    text = TextField


def run_twitter_etl():
    print("in etl")
    try:
        client = tweepy.Client(bearer_token=credentials.bearer_token)
        query = '("xbox" "usa")'

        response = client.search_recent_tweets(query=query, max_results=100,
                                               tweet_fields=['author_id', 'created_at', 'text', 'lang'])
        json_response = pd.DataFrame([i.data for i in response.data]).to_json(orient="records")
        json_response = json.loads(json_response)

        db = connect_to_db()
        existing_ids = get_stored_ids(db)

        counter = 0
        for i in json_response:
            if i["id"] not in existing_ids:
                db.save(i)
                counter += 1

        print("# of Tweets added from this response: ", counter)
    except Exception as e:
        print(e)


def connect_to_db():
    couch = couchdb.Server()
    couch.resource.credentials = (credentials.couch_usr, credentials.couch_pass)
    if credentials.db_name not in couch:
        couch.create(credentials.db_name)
    return couch[credentials.db_name]


def get_stored_ids(db):
    stored_ids = []
    if len(db) > 0:
        for d in db.view("_all_docs"):
            stored_ids.append(Tweet.load(db, d.id)._data["id"])
    return stored_ids

run_twitter_etl()