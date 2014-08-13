__author__ = 'mtenney'
import simplejson as json

def dump_tweet(tweet):
    j = json.dumps(tweet, indent=4)
    f = open('./tweet_dump.json', 'a')
    json.dump(j,f)
    f.close()
def dump_user(user):
    j = json.dumps(user, indent=4)

    f = open('./user_dump.json', 'a')

    json.dump(j,f)
    f.close()