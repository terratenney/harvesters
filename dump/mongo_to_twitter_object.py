__author__ = 'mtenney'
import twitter

def mongo_tweet_to_status(tweet):
    status = twitter.Status().NewFromJsonDict(tweet)
    return status

def mongo_user_to_user(user):
    user = twitter.User().NewFromJsonDict(user)
    return user
