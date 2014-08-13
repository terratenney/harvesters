__author__ = 'mtenney'
import pymongo

con = pymongo.MongoClient()
db = con.tweets
tweets = db.tweets_toronto
users = db.twitter_users

user_ids = list(users.find().distinct('id'))
def move_user_tags():
    count = 0
    for tweet in tweets.find():
        user_rec = tweet['user']
        print user_rec
        if isinstance(user_rec,(int,long)):
            print count
            count += 1
            tweet['user']= {'id':user_rec}
            tweets.update({'id':tweet['id']},tweet, upsert=True)
        elif isinstance(user_rec,dict):
            if len(user_rec.keys()) > 1:
                print count
                count += 1
                tweet['user']= {'id':user_rec['id']}
                print tweet['user']
                tweets.update({'id':tweet['id']},tweet, upsert=True)
                twitter_user_collection.update_user_modified_time(user_rec)
                try:
                    if user_rec['id'] not in user_ids:
                        users.insert({'id':user_rec['id']},user_rec)
                except:
                    pass
            elif len(user_rec.keys()) == 1 and user_rec.has_key('id'):
                        twitter_user_collection.update_user_modified_time(user_rec)
        else:
            print tweet

move_user_tags()
