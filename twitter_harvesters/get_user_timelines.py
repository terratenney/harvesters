__author__ = 'mtenney'
__author__ = 'mtenney'
from multiprocessing import Process
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dump import *
from pymongo import MongoClient
import twitter
from rate_limit_regulator import *
import threading
from key_files import twitter_keys
import cPickle as pickle

# ##############################################################################################################
conn = MongoClient('mongodb://localhost:27017/')
db = conn.tweets
db.tweets_toronto.ensure_index('id', unique=True, drop_dups=True, sparse=True)
db.twitter_users.ensure_index('id', unique=True, drop_dups=True, sparse=True)
tweets_toronto = db.tweets_toronto
twitter_users = db.twitter_users
##############################################################################################################

api = twitter.Api(twitter_keys.twitter_halifax_CONSUMER_KEY, twitter_keys.twitter_halifax_CONSUMER_SECRET,
                  twitter_keys.twitter_halifax_ACCESS_TOKEN, twitter_keys.twitter_halifax_ACCESS_SECRET,
                  debugHTTP=True)

api1 = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                   twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                   debugHTTP=True)
##############################################################################################################

lost_tweets = []


@rate_limited(62)
def get_user_timeline(user_id):
    try:

        tweets = api.GetUserTimeline(user_id=user_id, screen_name=None, since_id=None, max_id=None, count=5200,
                                  include_rts=True, trim_user=False, exclude_replies=False)

        # ets = api.GetUserTimeline(user_id=user_id, screen_name=None, since_id=None, max_id=None, count=5200,
        #                           include_rts=True, trim_user=False, exclude_replies=False)
        # tweets = twe.extend(ets)
        # print len(tweets), '\n\n\n\n\n'

    except:
        pass
        print "c"
    return tweets


def get_search_list():
    # search_list = []
    # search_list = search_list+get_followers_lists()
    # search_list = search_list+get_current_user_list()
    # search_list = search_list+get_friends_lists()
    # search_list = list(set(search_list))
    #search_list = tweets_toronto.find({'geo.coordinates': {"$within": {"$center": [[43.6, -79.9], 1]}}})
    user_ids = tweets_toronto.find({'geo.coordinates':{"$within": {"$center": [[44, -79], 2]}}}).distinct('user.id')
    userd= []
    for u in user_ids:
        search_list = list(twitter_users.find({'id':u}).limit(1))
        for user in search_list:
            if user.has_key('followers_list'):
                userd.extend(user['followers_list'])
            if user.has_key('friends_list'):
                userd.extend(user['friends_list'])
    search_list = userd
    return search_list


def get_current_user_list():
    user_ids = twitter_users.find().distinct('id')
    return user_ids


def get_friends_lists():
    f_list = []
    for user in twitter_users.find():
        if user.has_key('friends_list'):
            f_list = f_list + user['friends_list']
    l = len(f_list)
    f_list = set(f_list)
    d = len(f_list)
    print "%s Common Friends \n" % (l - d)
    return list(f_list)


def get_followers_lists():
    f_list = []
    for user in twitter_users.find():
        if user.has_key('followers_list'):
            f_list = f_list + user['followers_list']
    l = len(f_list)
    f_list = set(f_list)
    d = len(f_list)
    print "%s Common Friends \n" % (l - d)
    return list(f_list)


##############################################################################################################

##############################################################################################################

def run_get_user_timelines():
    search_list = get_search_list()
    print "Need to find %s user_timeslines" % len(search_list)
    tweets = None
    c = len(search_list)
    for user_id in search_list:

        print c
        c+= -1
        try:
            tweets = get_user_timeline(user_id)
        except:
            pass
        tweet_ds = twitter_users.find().distinct('id')
        user_ids = get_current_user_list()
        if tweets:

            for tweet in tweets:
                tweet = tweet.AsDict()
                user = tweet['user']
                tweet['user'] = {'id': user['id']}
                try:
                    if tweet['id'] in tweet_ds:
                        pass
                    else:
                        tweets_toronto.update({'id': tweet['id']}, tweet, upsert=True)
                except:
                    print "Couldn't save tweet", tweet['id']

                    lost_tweets.append(tweet)
                    dump_tweet(tweet)
                if user['id'] in user_ids:
                    pass
                else:
                    twitter_users.update({'id': user['id']}, user, upsert=True)
                    print "Added New User"

#############################################################################################################



@rate_limited(62)
def get_user_timeline1(user_id):
    try:
        # ctwts = tweets_toronto.find({'user.id': user_id}).distinct('id').sort()
        tweets = api.GetUserTimeline(user_id=user_id, screen_name=None, since_id=None, max_id=None, count=5200,
                                  include_rts=True, trim_user=False, exclude_replies=False)
        # ets = api.GetUserTimeline(user_id=user_id, screen_name=None, since_id=None, max_id=ctwts[-1], count=5200,
        #                           include_rts=True, trim_user=False, exclude_replies=False)
        # tweets = twe.extend(ets)
        # print len(tweets), '\n\n\n\n\n'

    except:
        pass
    return tweets


def run_get_user_timelines1():
    search_list = get_search_list()
    print "Need to find %s user_timeslines" % len(search_list)
    c = len(search_list)
    for user_id in search_list[::-1]:
        print c
        c-= -1
        try:
            tweets = get_user_timeline1(user_id)
        except:
            print 'nope'
            tweets = None
            pass
        user_ids = get_current_user_list()
        tweet_ds = twitter_users.find().distinct('id')

        if tweets:
            for tweet in tweets:
                tweet = tweet.AsDict()
                user = tweet['user']
                tweet['user'] = {'id': user['id']}
                try:
                    if tweet['id'] in tweet_ds:
                        pass
                    else:
                        tweets_toronto.update({'id': tweet['id']}, tweet, upsert=True)
                except:
                    print "Couldn't save tweet", tweet['id']
                    lost_tweets.append(tweet)
                    dump_tweet(tweet)

                if user['id'] in user_ids:
                    pass
                else:
                    twitter_users.update({'id': user['id']}, user, upsert=True)
                    print "Added New User"


if __name__ == '__main__':
    p1 = Process(target=run_get_user_timelines, args=(), name='GettingUserTimelines').start()
    p2 = Process(target=run_get_user_timelines1, args=(), name='GettingUserTimelines1').start()