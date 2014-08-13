__author__ = 'mtenney'
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from multiprocessing import Process
from pymongo import MongoClient
import twitter
from rate_limit_regulator import *
import threading
from key_files import twitter_keys
import cPickle as pickle
from dump import *
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
###############################################################################################################

@rate_limited(8)
def get_user_profile(user_id):
    try:
        u = api.GetUser(user_id=user_id)
        try:
            twitter_users.update({'id':u.GetId()}, u.AsDict(), upsert=True)
            print "\n\n\n"
        except:
            print "\n\nCouldn't Update Database - saved to pickle"
            dump_user(u.AsDict())
    except:
        print "Unexpected error:", sys.exc_info()[0]

def check_user_profiles():
    user_keys = [u'follow_request_sent', u'profile_use_background_image', u'id', u'verified',
                 u'profile_image_url_https', u'profile_sidebar_fill_color',
                 u'is_translator', u'geo_enabled', u'followers_count', u'protected', u'location',
                 u'default_profile_image', u'id_str', u'utc_offset',
                 u'statuses_count', u'description', u'friends_count',  u'profile_image_url',
                 u'notifications', u'screen_name', u'name', u'lang',
                 u'favourites_count', u'listed_count', u'url', u'created_at',
                 u'contributors_enabled', u'time_zone',
                 u'default_profile', u'following', u'_id']
    trouble_list = []
    for user in twitter_users.find():
        if user.keys() < user_keys:
            trouble_list.append(user['id'])
    for uid in trouble_list[::-1]:
        time.sleep(2)
        get_user_profile(uid)



###############################################################################################################

@rate_limited(8)
def get_user_profile(user_id):
    try:
        u = api1.GetUser(user_id=user_id)
        try:
            twitter_users.update({'id': u.GetId()}, u.AsDict(), upsert=True)
            print "\n\n\n"
        except:
            print "Couldn't Update Database - saved to pickle"
            dump_user(u.AsDict())
    except:
        print "Unexpected error:", sys.exc_info()[0]


def check_user_profiles1():
    user_keys = [u'follow_request_sent', u'profile_use_background_image', u'id', u'verified',
                 u'profile_image_url_https', u'profile_sidebar_fill_color',
                 u'is_translator', u'geo_enabled', u'followers_count', u'protected', u'location',
                 u'default_profile_image', u'id_str', u'utc_offset',
                 u'statuses_count', u'description', u'friends_count',  u'profile_image_url',
                 u'notifications', u'screen_name', u'name', u'lang',
                 u'favourites_count', u'listed_count', u'url', u'created_at',
                 u'contributors_enabled', u'time_zone',
                 u'default_profile', u'following', u'_id']
    trouble_list = []
    for user in twitter_users.find():
        if user.keys() < user_keys:
            trouble_list.append(user['id'])
    for uid in trouble_list:
        time.sleep(2)
        get_user_profile(uid)
if __name__ == '__main__':
    p1 = Process(target=check_user_profiles1, name='CheckUserProfiles').start()
    p2 = Process(target=check_user_profiles, name='CheckUserProfiles').start()