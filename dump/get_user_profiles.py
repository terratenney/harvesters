# __author__ = 'mtenney'
# import pymongo
# import twitter
#
# import os
# import sys
# from basic_administration.rate_limit_regulator import *
# sys.path.append( os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# import twitter_keys
# import cPickle as pickle
#
# api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
#                       twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
#                       debugHTTP=True)
# con = pymongo.MongoClient()
#
# users = con.tweets.twitter_users
#
# def get_user_profiles():
#     user_lookup = []
#     for user in users.find():
#         if len(user.keys()) < 5:
#             user_lookup.append(user['id'])
#     return user_lookup
# fp = open('D:\\data\\dead_twitter_users.spkl','wb')
# @rate_limited(12)
# def grab_user_profiles(l):
#     users.remove({'id':l})
#     u = api.GetUser(l)
#     print u
#     try:
#         users.update({'id':u.GetId()},u.AsDict(),upsert=True)
#     except:
#         pickle.dump(u,fp)
#
# if __name__ == '__main__':
#     l = get_user_profiles()
#
#     count = len(l)
#     print 'Count: ', count
#     for n in l:
#         grab_user_profiles(n)
#         count += -1
#         print "Count: ", count