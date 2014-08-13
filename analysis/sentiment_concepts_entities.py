from alchemyapi_python import alchemyapi

api = alchemyapi.AlchemyAPI()

import pymongo
from dump.rate_limit_regulator import *


con = pymongo.MongoClient()
tweets = con.tweets.tweets_toronto
entity_mentions = con.tweets.entity_results
sentiment = con.tweets.sentiment_analysis
concepts = con.tweets.concepts_found

@RateLimited(7)
def find_entities(text):
    res = api.entities('text',text, options={'disambiguate':1, 'linkedData':1 , 'coreference': 1, 'quotations': 1, 'sentiment': 1, 'maxRetrieve': 200})
    try:
        res.pop('usage')
    except:
        pass
    return res
@RateLimited(7)
def calc_sentiment(text):
    res = api.sentiment('text',text)
    try:
        res.pop('usage')
    except:
        pass
    return res
@RateLimited(7)
def find_concepts(text):
    res = api.concepts('text',text, options={'maxRetrieve':1000})
    try:
        res.pop('usage')
    except:
        pass
    return res



t_ids = entity_mentions.find().distinct('t_id')
t_ids = t_ids+sentiment.find().distinct('t_id')
t_ids = t_ids+concepts.find().distinct('t_id')
t_ids = set(t_ids)

for tweet in tweets.find():
    if tweet['id'] not in t_ids and tweet.has_key('text'):
        text = tweet['text']
        res = find_entities(text)
        res['t_id']= tweet['id']
        entity_mentions.update({'id': res['t_id']},res, upsert=True)
        res = calc_sentiment(text)
        res['t_id'] = tweet['id']
        sentiment.update({'id': res['t_id']},res, upsert=True)
        res = find_concepts(text)
        res['t_id'] = tweet['id']
        concepts.update({'id': res['t_id']},res, upsert=True)
    else:
        if not tweet.has_key('text'):
            print tweet
