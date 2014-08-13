__author__ = 'mtenney'
jformat = {"displayFieldName" : "Users",
           "fieldAliases" :
                {"tweet_id" : "Tweet ID",
                "user_id" : "Twitter_user_id"},
"geometryType" : "Point",
"hasZ" : False,
"hasM" : False,
"spatialReference" :{"wkid": 4326},
"fields": [{
                "name": "tweet_id",
                "type": "Long",
                "alias": "Tweet ID"
            },
            {
                "name": "user_id",
                "type": "Long",
                "alias": "User ID"
            }],
"features":[]}


import pymongo

con = pymongo.MongoClient()
tweets = con.tweets.tweets_toronto

for tweet in tweets.find({'geo.coordinates': {"$within": {"$center": [[44, -79], .9]}}}).limit(1000):

    tweet['geo'] = {
"x" : tweet['geo']['coordinates'][1], "y" : tweet['geo']['coordinates'][0]}
    rec = {'geometry': tweet['geo'],
           "attributes": {
                    "user_id":tweet['user']['id'],
                    "tweet_id": tweet['id']}}
    jformat['features'].append(rec)

import json
with open('./test.json', 'w') as outfile:
    json.dump(jformat, outfile, indent=4)
    outfile.close()
