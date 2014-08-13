from key_files import twitter_keys

__author__ = 'mtenney'
def update_user_tweet_counts():
    con = pymongo.MongoClient()
    db = con.tweets
    toronto_col = db.tweets_toronto
    example_users = db.example_users
    for user in twitter_users.find():
        o_user = user
        if user.has_key('tweet_ids'):
             tweets = list(toronto_col.find({'user.id':user['id']}))
             tweet_ids = [tweet['id'] for tweet in tweets]
             user['collected_statuses']= len(tweet_ids)
             user['tweet_ids']=tweet_ids
        else:
             user['tweet_ids']= []
             tweets = list(toronto_col.find({'user.id':user['id']}))
             tweet_ids = [tweet['id'] for tweet in tweets]
             user['collected_statuses']= len(tweet_ids)
             user['tweet_ids']= tweet_ids
        twitter_users.update({'id':user["id"]},user)
    print "Updated collected tweet counts for users"
    threading.Timer(120,update_user_tweet_counts).start()



def update_user_tweet_index():
    con = pymongo.MongoClient()
    db = con.tweets
    torontostream = db.tweets_toronto
    twitter_users = db.twitter_users
    for user in twitter_users.find():
        user_id = user['id']
        if user.has_key('tweet_ids'):
            statuses = list(torontostream.find({'user.id': user_id}).sort('created_at', pymongo.ASCENDING))
            user['tweet_ids'] = [status['id'] for status in statuses]
            user['collected_statuses'] = len(user['tweet_ids'])
        else:
            user['tweet_ids'] =[]
            user['collected_statuses'] = []
            statuses = list(torontostream.find({'user.id': user_id}).sort('created_at', pymongo.ASCENDING))
            user['tweet_ids'] = [status['id'] for status in statuses]
            user['collected_statuses'] = len(user['tweet_ids'])
        twitter_users.update({'id': user['id']}, user, upsert=True)
    print "Updated User Status List Index"
    threading.Timer(901, update_user_tweet_index).start()

# def load_tweets(tweet_dir):
#     tweet_files = os.listdir(tweet_dir)
#     #print header_map
#
#     tweets = []
#     for f in tweet_files:
#         file_path = os.path.join(tweet_dir, f)
#         with open(file_path,'r') as csvfile:
#             csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
#             csvreader.next() # Skip header
#             for row in csvreader:
#                 tweets.append(row)
#
#     print 'Loaded %d tweets' % len(tweets)
#
#     #print tweets[:10]
#
#     return tweets




__author__ = 'mtenney'

import twitter
import pymongo

from dump.rate_limit_regulator import *


con = pymongo.MongoClient()
db = con.tweets
toronto_stream = db.tweets_toronto

twitter_users = db.twitter_users

api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                      twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                      debugHTTP=True)

api.GetBlocks()
@rate_limited(12)
def get_profile(userid):
    user = api.GetUser(user_id=userid)
    user_ids = twitter_users.find().distinct('id')
    if user.GetId in user_ids:
        pass
    else:
        try:
            twitter_users.insert({'id':user.GetId()},user.AsDict())
            print "\n\n\t Saved user ", user.GetId()
        except:
            print userid

    count = 0
    user_ids = twitter_users.find().distinct('id')
    for user in twitter_users.find():
        if user.has_key('friends_list'):
            for friend in user['friends_list']:
                if friend in user_ids:
                    pass
                else:
                    get_profile(friend)


    for user in twitter_users.find():
        if user.has_key('followers_list'):
            for friend in user['followers_list']:
                if friend in user_ids:
                    pass
                else:
                    get_profile(friend)