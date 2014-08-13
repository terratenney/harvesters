__author__ = 'mtenney'
import pymongo

def get_tweets_con():
    con = pymongo.MongoClient()
    db = con.tweets
    tweets = db.tweets_toronto
    return tweets


def get_user_con():
    con = pymongo.MongoClient()
    db = con.tweets
    users = db.tiwtter_users
    return users