
__author__ = 'mtenney'
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
import twitter

from rate_limit_regulator import *

from key_files import twitter_keys


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


#######################################################################################################################

def RateLimited(maxPerMinute):
    minInterval = 60.0 / float(maxPerMinute)

    def decorate(func):
        lastTimeCalled = [0.0]

        def rateLimitedFunction(*args, **kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                time.sleep(leftToWait)
            ret = func(*args, **kargs)
            lastTimeCalled[0] = time.clock()
            return ret

        return rateLimitedFunction

    return decorate


###################################################################################################################

def rate_limited(maxPerMinute):
    """
    Decorator that make functions not be called faster than
    """
    lock = threading.Lock()
    minInterval = 60.0 / float(maxPerMinute)

    def decorate(func):
        lastTimeCalled = [0.0]

        def rateLimitedFunction(*args, **kargs):
            lock.acquire()
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                time.sleep(leftToWait)
            lock.release()
            ret = func(*args, **kargs)
            lastTimeCalled[0] = time.clock()
            return ret

        return rateLimitedFunction

    return decorate


#######################################################################################################################

def mongo_tweet_to_status(tweet):
    status = twitter.Status().NewFromJsonDict(tweet)
    return status


def mongo_user_to_user(user):
    user = twitter.User().NewFromJsonDict(user)
    return user


######################################################################################################################

def update_user_modified_time(user):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    if user.has_key('last_update'):
        current_time = time.strftime('%a %b %d %H:%M:%S +0000 %Y', time.strptime(current_time, '%Y-%m-%d %H:%M:%S'))
        user['last_update'] = current_time
    else:
        current_time = time.strftime('%a %b %d %H:%M:%S +0000 %Y', time.strptime(current_time, '%Y-%m-%d %H:%M:%S'))
        user['last_update'] = current_time
        print "Changing last update time"
    return user


######################################################################################################################



######################################################################################################################


#######################################################################################################################


######################################################################################################################
def move_user_tags():
    count = 0
    user_ids = twitter_users.find().distinct('id')
    for tweet in tweets.find():
        user_rec = tweet['user']
        print user_rec

        if isinstance(user_rec, (int, long)):
            print count
            count += 1
            tweet['user'] = {'id': user_rec}
            tweets.update({'id': tweet['id']}, tweet, upsert=True)

        elif isinstance(user_rec, dict):

            if len(user_rec.keys()) > 1:
                print count
                count += 1
                tweet['user'] = {'id': user_rec['id']}
                print tweet['user']
                tweets.update({'id': tweet['id']}, tweet, upsert=True)
                update_user_modified_time(user_rec)
                try:
                    if user_rec['id'] not in user_ids:
                        twitter_users.insert({'id': user_rec['id']}, user_rec)
                except:
                    pass
            elif len(user_rec.keys()) == 1 and user_rec.has_key('id'):
                update_user_modified_time(user_rec)
        else:
            print tweet


#######################################################################################################################

def update_collected_statuses():
    count = 0
    for user in twitter_users.find():
        collected_statuses = list(tweets.find({'user.id': user['id']}).distinct('id'))
        user['collected_statuses'] = len(collected_statuses)
        user['tweet_ids'] = collected_statuses
        twitter_users.update({'id': user['id']}, user)
        count += 1
    print "Updated %s out of %s total users" % (count, twitter_users.count())
    threading.Timer(update_collected_statuses, 1800).start()

#######################################################################################################################

def get_user_timelines(user):
    user_ids = twitter_users.find().dinstinct('id')
