__author__ = 'mtenney'

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
import twitter
from rate_limit_regulator import *
import threading
from key_files import twitter_keys
import cPickle as pickle

###############################################################################################################
conn = MongoClient('mongodb://localhost:27017/')
db = conn.tweets
db.tweets_toronto.ensure_index('id', unique=True, drop_dups=True, sparse=True)
db.twitter_users.ensure_index('id', unique=True, drop_dups=True, sparse=True)
tweets = db.tweets_toronto
twitter_users = db.twitter_users
##############################################################################################################

api = twitter.Api(twitter_keys.twitter_halifax_CONSUMER_KEY, twitter_keys.twitter_halifax_CONSUMER_SECRET,
                  twitter_keys.twitter_halifax_ACCESS_TOKEN, twitter_keys.twitter_halifax_ACCESS_SECRET,
                  debugHTTP=True)

api1 = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                  twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                  debugHTTP=True)
###############################################################################################################
#fp = open('./user_pickles/friend_user_records.spkl','wb')
###############################################################################################################

@rate_limited(1)
def get_followers(user):
    """
    This function goes through each user in the database and updates the people the people they are being followed by. Put in
    user['followers_list']
    """
    if user.has_key('followers_list'):
        pass
    else:
        if user.has_key('followers_count'):
            if user['followers_count'] > 4999:
                pages = user['followers_count'] / 5000
                f_list = []
                for page in range(pages):
                    try:
                        follower_set = api.GetFollowers(user_id=user['id'], cursor=page, count=5000)
                        friends_list = []
                        for follower in follower_set:
                            twitter_users.update({'id':follower.GetId()},follower.AsDict(),upsert=True)
                            friends_list.append(follower.GetId())
                        f_list = friends_list + f_list
                        time.sleep(60)
                        user['followers_list'] = f_list
                        twitter_users.update({'id': user['id']}, user)
                        print "\n\nGot %s followers out of %s listed" % (len(f_list), user['followers_count'])
                    except Exception, e:
                        print str(e)
                        time.sleep(60)
            else:
                try:
                    follower_set = api.GetFollowers(user_id=user['id'], count=5000)
                    friends_list = []
                    for follower in follower_set:
                        twitter_users.update({'id':follower.GetId()},follower.AsDict(),upsert=True)
                        friends_list.append(follower.GetId())
                    user['followers_list'] = friends_list
                    twitter_users.update({'id': user['id']}, user)
                    print "\n\nGot %s followers out of %s listed" % (len(friends_list), user['followers_count'])
                except Exception, e:
                    print str(e)
                    time.sleep(60)


def run_get_followers():
    user_ids = tweets.find({'geo.coordinates':{"$within": {"$center": [[44, -79], 1]}}}).distinct('user.id')
    for user in user_ids:
        user = twitter_users.find_one({'id':user})
        if user.has_key('followers_count') and not user.has_key('followers_list'):
            get_followers(user)




###############################################################################################################

@rate_limited(1)
def get_followers1(user):
    """
    This function goes through each user in the database and updates the people the people they are being followed by. Put in
    user['followers_list']
    """
    if user.has_key('followers_list'):
        pass
    else:
        if user.has_key('followers_count'):
            if user['followers_count'] > 4999:
                pages = user['followers_count'] / 5000
                f_list = []
                for page in range(pages):
                    try:
                        follower_set = api1.GetFollowers(user_id=user['id'], cursor=page, count=5000)
                        friends_list = []
                        for follower in follower_set:
                            twitter_users.update({'id':follower.GetId()},follower.AsDict(),upsert=True)
                            friends_list.append(follower.GetId())
                        f_list = friends_list + f_list
                        time.sleep(60)
                        user['followers_list'] = f_list
                        twitter_users.update({'id': user['id']}, user)
                        print "\n\nGot %s followers out of %s listed" % (len(f_list), user['followers_count'])
                    except Exception, e:
                        print str(e)
                        time.sleep(60)
            else:
                try:
                    follower_set = api1.GetFollowers(user_id=user['id'], count=5000)
                    friends_list = []
                    for follower in follower_set:
                        twitter_users.update({'id':follower.GetId()},follower.AsDict(),upsert=True)
                        friends_list.append(follower.GetId())
                    user['followers_list'] = friends_list
                    twitter_users.update({'id': user['id']}, user)
                    print "\n\nGot %s followers out of %s listed" % (len(friends_list), user['followers_count'])
                except Exception, e:
                    print str(e)
                    time.sleep(60)


def run_get_followers1():
    user_ids = tweets.find({'geo.coordinates':{"$within": {"$center": [[44, -79], 1]}}}).distinct('user.id')
    for user in user_ids[::-1]:
        user = twitter_users.find_one({'id':user})
        if user.has_key('followers_count') and not user.has_key('followers_list'):
            get_followers1(user)


threading.Thread(target=run_get_followers1, name="GetFollowers").start()
threading.Thread(target=run_get_followers, name="GetFollowers").start()