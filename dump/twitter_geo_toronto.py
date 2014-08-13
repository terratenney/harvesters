import os
import sys

from pymongo import MongoClient
import twitter

from key_files import twitter_keys


sys.path.append( os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                      twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                      debugHTTP=True)
    conn = MongoClient('mongodb://localhost:27017/')
    db = conn.tweets
    db.tweets_toronto.ensure_index('id', unique=True, drop_dups=True, sparse=True)
    db.twitter_users.ensure_index('id', unique=True, drop_dups=True, sparse=True)
    tweets = db.tweets_toronto
    twitter_users = db.twitter_users
    count = 0


    for tweet in api.GetStreamFilter(locations=["-80", "42.9", "-78.5", "44.2"]):
        idstr = tweet['id']
        user_id = tweet['user']['id']

        user = tweet['user']
        tweet['user'] = {'id': user['id']}

        user_ids = twitter_users.find().distinct('id')

        if user_id in user_ids:
            print '\n'
            pass
        else:

            twitter_users.update({'id':user['id']},user,upsert=True)

            print "\n\t", user

        tweets.update({'id': idstr}, tweet, upsert=True)
        count += 1
        print "Collected %s tweets from Toronto!" % str(count)


if __name__ == '__main__':
    main()
