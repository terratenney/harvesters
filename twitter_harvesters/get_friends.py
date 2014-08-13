__author__ = 'mtenney'
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
def get_friends1(user):
    if user.has_key('friends_count'):
        if user.has_key('friends_list'):
            print user
        else:
            if user['friends_count'] > 4999:
                pages = user['friends_count'] / 5000
                f_list = []
                for page in range(pages):
                    try:
                        friends = api1.GetFriends(user_id=user['id'], cursor=page, count=5000)
                        friends_list = []
                        for friend in friends:
                            twitter_users.update({'id':friend.GetId()},friend.AsDict(),upsert=True)
                            friends_list.append(friend.GetId())
                        user['friends_list'] = friends_list
                        twitter_users.update({'id': user['id']}, user)
                        time.sleep(60)
                        print "\n\nGot %s friends out of %s listed" % (len(f_list), user['friends_count'])
                    except Exception, e:
                        print str(e)
            else:
                try:
                    friends = api1.GetFriends(user_id=user['id'], count=5000)
                    friends_list = []
                    for friend in friends:
                        twitter_users.update({'id':friend.GetId()},friend.AsDict(),upsert=True)
                        friends_list.append(friend.GetId())
                    user['friends_list'] = friends_list
                    twitter_users.update({'id': user['id']}, user)
                    print "\n\nGot %s friends out of %s listed" % (len(friends_list), user['friends_count'])
                except Exception, e:
                    print str(e)


@rate_limited(1)
def get_friends(user):
    if user.has_key('friends_count'):
        if user.has_key('friends_list'):
            print user
        else:
            if user['friends_count'] > 4999:
                pages = user['friends_count'] / 5000
                f_list = []
                for page in range(pages):
                    try:
                        friends = api.GetFriends(user_id=user['id'], cursor=page, count=5000)
                        friends_list = []
                        for friend in friends:
                            twitter_users.update({'id':friend.GetId()},friend.AsDict(),upsert=True)
                            friends_list.append(friend.GetId())
                        user['friends_list'] = friends_list
                        twitter_users.update({'id': user['id']}, user)
                        print "\n\nGot %s friends out of %s listed" % (len(f_list), user['friends_count'])
                        time.sleep(60)
                    except Exception, e:
                        print str(e)
            else:
                try:
                    friends = api.GetFriends(user_id=user['id'], count=5000)
                    friends_list = []
                    for friend in friends:
                        twitter_users.update({'id':friend.GetId()},friend.AsDict(),upsert=True)
                        friends_list.append(friend.GetId())
                    user['friends_list'] = friends_list
                    twitter_users.update({'id': user['id']}, user)
                    print "\n\nGot %s friends out of %s listed" % (len(friends_list), user['friends_count'])
                except Exception, e:
                    print str(e)


def run_get_friends():
    user_ids = tweets.find({'geo.coordinates':{"$within": {"$center": [[44, -79], 1]}}}).distinct('user.id')
    for user in user_ids:
        user = twitter_users.find_one({'id':user})
        if user.has_key('friends_count') and not user.has_key('friends_list'):
            get_friends(user)

def run_get_friends1():
    user_ids = tweets.find({'geo.coordinates':{"$within": {"$center": [[44, -79], 1]}}}).distinct('user.id')
    for user in user_ids[::-1]:
        user = twitter_users.find_one({'id':user})
        if user.has_key('friends_count') and not user.has_key('friends_list'):
            get_friends1(user)

threading.Thread(target=run_get_friends, name='GettingFriendsList').start()
threading.Thread(target=run_get_friends1, name='GettingFriendsList1').start()