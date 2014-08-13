from foursquare_harvester import foursquare_keys

__author__ = 'mtenney'
import foursquare
import pymongo
from rate_limit_regulator import *

con = pymongo.MongoClient()
db = con.tweets
tus = db.twitter_users

user_names = list(tus.find({'collected_statuses':{'$gte':500}}).distinct("screen_name"))
fqu = db.foursq_links




@rate_limited(.85)
def find_users(user):
    client = foursquare.Foursquare(client_id=foursquare_keys.YOUR_CLIENT_ID, client_secret=foursquare_keys.YOUR_CLIENT_SECRET, access_token=foursquare_keys.ACCESS_TOKEN)
    res = client.users.search(params={'twitter':user})['results']
    if res and len(res)==1:
        id = int(res[0]['id'])
        assert isinstance(id,int)
        try:
            fqu.update({'id':id},res[0],upsert=True)
        except:
            pass
def run():
    v = True
    count = len(user_names)
    client = foursquare.Foursquare(client_id=foursquare_keys.YOUR_CLIENT_ID, client_secret=foursquare_keys.YOUR_CLIENT_SECRET, access_token=foursquare_keys.ACCESS_TOKEN)
    users = iter(user_names)
    while users:
        user =users.next()
        find_users(user)
        user_names.remove(user)
        count += -1
        print "Twitter name searches left", count

if __name__ == "__main__":
    run()

