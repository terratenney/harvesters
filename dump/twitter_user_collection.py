from key_files import twitter_keys

__author__ = 'mtenney'

import twitter
import pymongo

from dump.rate_limit_regulator import *
from dump.update_modification_time import *




@RateLimited(10)
def get_user_timeline(api, user):
    con = pymongo.MongoClient()
    db = con.tweets
    torontostream = db.tweets_toronto
    twitter_users = db.twitter_users
    time.sleep(3)
    tweets = list(torontostream.find({'user.id':user['id']}))
    tweet_ids = [tweet['id'] for tweet in tweets]
    tweet_id = tweet_ids[0]
    e_tweet_id = tweet_ids[-1]

    user['statuses_count'] = api.GetUser(user_id=user['id']).GetStatusesCount()

    diff = abs(user['collected_statuses'] - user['statuses_count'])
    print "Found we need to get # %s tweets for %s" % (diff, user['id'])
    if diff < 3200:

        time.sleep(2)
        front = api.GetUserTimeline(user['id'], since_id=0, count= 3200)
        for tweet in front:
            tweet = tweet.AsDict()
            tweet['user'] = user['id']
        try:
            twitter_users.update({'id': user['id']}, user, upsert=True)
        except:
            print 'Couldnt update user',user['id']
        try:
            torontostream.update({'id': tweet['id']}, tweet, upsert=True)
        except:
             print 'Couldnt update tweet',tweet['id']

        time.sleep(3)

        back = api.GetUserTimeline(user['id'], since_id=tweet['id'], count=3200)
        for tweet in back:
            tweet = tweet.AsDict()
            tweet['user'] = user['id']
        try:
            twitter_users.update({'id': user['id']}, user, upsert=True)
        except:
            print 'Couldnt update user',user['id']
        try:
            torontostream.update({'id': tweet['id']}, tweet, upsert=True)
        except:
            print 'Couldnt update tweet ', tweet['id']
    elif diff == 0:
        print "No need to find more tweets for ", user['name']
    else:
        sid =0
        for i in range(user['statuses_count'] / 3200):
            chunk = api.GetUserTimeline(user_id=user['id'],since_id=sid,count=3200)
            if isinstance(chunk, bool) or len(chunk) < 1:
                continue
            else:
                for tweet in chunk:
                    tweet = tweet.AsDict()
                    tweet['user'] = user['id']
                    try:
                        twitter_users.update({'id': user['id']}, user, upsert=True)
                    except:
                       print '\tCouldnt update user ', user['id']
                    try:
                        torontostream.update({'id': tweet['id']}, tweet, upsert=True)
                    except:
                        print '\tCouldnt update tweet ', tweet['id']
            sid = chunk[-1].AsDict()['id']
lost_tweets =[]
@RateLimited(1)
def update_user_profiles(oldest=None, Counts_Upated= False):
    api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                      twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                      debugHTTP=True)
    con = pymongo.MongoClient()
    db = con.tweets
    torontostream = db.tweets_toronto
    twitter_users = db.twitter_users
    if Counts_Upated is False:
        update_user_tweet_counts()
    if oldest:
        for user in list(twitter_users.find().sort('last_update', pymongo.ASCENDING)[0:oldest]):
            get_user_timeline(api, user)
            update_user_modified_time(user)
    else:
        for user in list(twitter_users.find().sort('last_update', pymongo.ASCENDING)):
            get_user_timeline(api, user)
            update_user_modified_time(user)
    threading.Timer(20, update_user_profiles,[10,False]).start()



if __name__ == '__main__':
    #update_user_tweet_counts()
    update_user_profiles(10,False)
    # update_user_tweet_index()