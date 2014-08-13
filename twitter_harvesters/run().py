__author__ = 'mtenney'


for user in example_users.find().distinct('id'):
    twts = get_user_timeline(user['id'])
    for tweet in twts:
        tweet = tweet.AsDict()
        tweet['user']= {'id':tweet['user']['id']}
        print tweet
        example_tweets.update({'id':tweet['id']},tweet, upsert=True)

