from key_files import twitter_keys

__author__ = 'mtenney'
import os
import sys

import twitter
import pymongo

from dump.rate_limit_regulator import *


con = pymongo.MongoClient()
db = con.tweets
toronto_stream = db.tweets_toronto
twitter_users = db.twitter_users
sys.path.append( os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def update_collected_statuses():
    count = 0
    for user in twitter_users.find():
        collected_statuses = list(toronto_stream.find({'user.id': user['id']}).distinct('id'))
        user['collected_statuses'] = len(collected_statuses)
        user['tweet_ids'] = collected_statuses
        twitter_users.update({'id': user['id']}, user)
        count += 1
    print "Updated %s out of %s total users" % (count, twitter_users.count())


@rate_limited(1)
def update_friends_list(user):
    '''
    This function goes through each user in the database and updates the people the people they follow. Put in
    user['friends_list']
    :return:
    '''
    api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                      twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                      debugHTTP=True)
    if user.has_key('friends_list'):
        pass
    else:
        if user['friends_count'] > 4999:
            pages = user['friends_count'] / 5000
            f_list = []
            for page in range(pages):
                try:
                    friend_set = api.GetFriendIDs(user_id=user['id'], cursor=page, count=5000)
                    f_list = friend_set + f_list
                    time.sleep(60)
                    user['friends_list'] = f_list
                    twitter_users.update({'id': user['id']}, user)
                    print "\n\nGot %s friends out of %s listed" % (len(f_list), user['friends_count'])
                except Exception, e:
                    print str(e)

        else:
            try:
                friends_list = api.GetFriendIDs(user_id=user['id'], count=5000)
                user['friends_list'] = friends_list
                twitter_users.update({'id': user['id']}, user)
                print "\n\nGot %s friends out of %s listed" % (len(friends_list), user['friends_count'])
            except Exception, e:
                    print str(e)

@rate_limited(1)
def update_follower_list(user):
    '''
    This function goes through each user in the database and updates the people the people they are being followed by. Put in
    user['followers_list']
    '''
    api = twitter.Api(twitter_keys.twitter_canada_CONSUMER_KEY, twitter_keys.twitter_canada_CONSUMER_SECRET,
                      twitter_keys.twitter_canada_ACCESS_TOKEN, twitter_keys.twitter_canada_ACCESS_SECRET,
                      debugHTTP=True)
    if user.has_key('followers_list'):
        pass
    else:
        if user.has_key('followers_count'):
            if user['followers_count'] > 4999:
                pages = user['followers_count'] / 5000
                f_list = []
                for page in range(pages):
                    try:
                        follower_set = api.GetFollowerIDs(user_id=user['id'], cursor=page, count=5000)
                        f_list = follower_set + f_list
                        time.sleep(60)
                        user['followers_list'] = f_list
                        twitter_users.update({'id': user['id']}, user)
                        print "\n\nGot %s followers out of %s listed" % (len(f_list), user['followers_count'])
                    except Exception, e:
                        print str(e)
                        time.sleep(60)
            else:
                try:
                    followers_list = api.GetFollowerIDs(user_id=user['id'], count=5000)
                    user['followers_list'] = followers_list
                    twitter_users.update({'id': user['id']}, user)
                    print "\n\nGot %s followers out of %s listed" % (len(followers_list), user['followers_count'])
                except Exception, e:
                        print str(e)
                        time.sleep(60)

@rate_limited(1)
def run_friends():
    count = 0
    for user in twitter_users.find():
        if user.has_key('friends_count'):
            update_friends_list(user)
            count += 1
            print "friends found %s" % count


@rate_limited(1)
def run_follower():
    count = 0
    for user in twitter_users.find():
        if user.has_key('followers_count'):
            update_follower_list(user)
            count += 1
            print "followers to go %s" % count


@rate_limited(3)
def check_threads(threads):
    for t in threads:
        print "\t %s is alive: %s" % (t.getName(), t.isAlive())
    threading.Timer(10, check_threads, [threads]).start()

@rate_limited(1)
def check_user_keys():
    print "Status"


if __name__ == "__main__":
    print "Maintenance DB script"
    t1 = threading.Thread(target=run_follower,name="Followers_Update")
    t2 = threading.Thread(target=run_friends,name="Friends_Update")
    t3 = threading.Thread(target=update_collected_statuses,name="Collected_Update")
    t3.start()
    t2.start()
    t1.start()
    check_threads([t1,t2,t3])