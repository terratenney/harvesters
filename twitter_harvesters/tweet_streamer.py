__author__ = 'mtenney'

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
import twitter

import threading
from harvesters.key_files import twitter_keys

#######################################################################################################################
conn = MongoClient('mongodb://localhost:27017/')
db = conn.tweets
db.tweets_toronto.ensure_index('id', unique=True, drop_dups=True, sparse=True)
db.twitter_users.ensure_index('id', unique=True, drop_dups=True, sparse=True)
tweets = db.tweets_toronto
twitter_users = db.twitter_users
#######################################################################################################################

api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                  twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                  debugHTTP=True)
##################################################################################################################
def streamer(location,track):
    count = 0
    for tweet in api.GetStreamFilter(locations=location,track=track):
        try:
            text = tweet['text']
            text=  text.encode('utf-8')
            tweet['text']=text
            print tweet
            idstr = tweet['id']
            user_id = tweet['user']['id']
            user = tweet['user']
            tweet['user'] = {'id': user['id']}
            user_ids = twitter_users.find().distinct('id')
            if user_id in user_ids:
                pass
            else:
                twitter_users.update({'id': user['id']}, user, upsert=True)
                print "Added New User"
            tweets.update({'id': idstr}, tweet, upsert=True)
            count += 1
            print "Collected %s tweets from Toronto!" % str(count)
        except:
            pass
location =["-80", "42.9", "-78.5", "44.2"]
track = 'montreal, Montreal'
#threading.Thread(target=streamer, verbose=True, args=(location,track,), name='Streamer').start()
#streamer(location)
##################################################################################################################
from nltk.tag.stanford import NERTagger
net = NERTagger('../stanford-ner-2014-06-16/classifiers/english.all.3class.distsim.crf.ser.gz','../stanford-ner-2014-06-16/stanford-ner.jar')

def search(term):
    twts=api.GetSearch(term=term, geocode=('45.5','-73.5','100mi'),count=2000,include_entities=True)
    count = 0
    for tweet in twts:
        tweet = tweet.AsDict()
        print tweet
        idstr = tweet['id']
        user_id = tweet['user']['id']
        user = tweet['user']
        tweet['user'] = {'id': user['id']}
        user_ids = twitter_users.find().distinct('id')
        if user_id in user_ids:
            pass
        else:
            twitter_users.update({'id': user['id']}, user, upsert=True)
            print "Added New User"
        tweets.update({'id': idstr}, tweet, upsert=True)
        count += 1
        print "Collected %s tweets from Toronto!" % str(count)

search(term=['IRA'])