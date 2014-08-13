__author__ = 'mtenney'
import pymongo
import folium
import vincent
import pandas as pd
import fiona
from fiona.crs import from_epsg
from textblob import TextBlob
from textblob_aptagger import PerceptronTagger
from textblob.sentiments import NaiveBayesAnalyzer
import datetime
from shapely import geometry
con = pymongo.MongoClient()
db = con.tweets
tweets_toronto = db.tweets_toronto
import os
os.chdir('../')
poly = fiona.open('D:\data\open_toronto\PROPERTY_BOUNDARIES_WGS84.shp')
from collections import Counter, OrderedDict
boros = {}

from alchemyapi_python import alchemyapi
api = alchemyapi.AlchemyAPI()



def calc_sentiment(text):
    res = api.sentiment('text', text)
    try:
        res.pop('usage')
    except:
        pass
def cal_sentiment(tweet_set):
    for tweet in tweet_set:
        tb = TextBlob(tweet['text'].lower(), pos_tagger=PerceptronTagger())
        nb = TextBlob(tweet['text'], analyzer=NaiveBayesAnalyzer())
        pol_tb = str(tb.polarity)
        sub_tb = str(tb.subjectivity)
        pol_nb = str(nb.polarity)
        sub_nb = str(nb.subjectivity)
        sent_p_nb = str(nb.sentiment[1])
        sent_n_nb = str(nb.sentiment[2])
        sent_c_nb = str(nb.sentiment[0])
        sent = [pol_tb, sub_tb, pol_nb, sub_nb, sent_p_nb, sent_n_nb, sent_c_nb]
        return sent


def make_recs(n, tweet_set):
    import fiona
    from textblob import TextBlob
    from textblob_aptagger import PerceptronTagger
    from textblob.sentiments import NaiveBayesAnalyzer
    records = []
    schema = {'geometry': 'Point', 'properties': {'id': 'str',
                                                  'tweet_id': 'str',
                                                  'user_id': 'str',
                                                  'created_at': 'str',
                                                  'text': 'str',
                                                  'pol_alc': 'str',
                                                  'sent_alc': 'str',
                                                  'pol_tb': 'str',
                                                  'sub_tb': 'str',
                                                  'sent_nltk': 'str',
                                                  'sen_pos_n': 'str',
                                                  'sen_neg_n': 'str',
                                                  'pol_nltk': 'str',
                                                  'sub_nltk': 'str',
                                                  'category': 'str',
                                                  'cat_conf': 'str',
                                                  'cat_url': 'str'}}
    shp = fiona.open("D:\\data\\temp\\test"+str(n)+".shp", 'w', 'ESRI Shapefile', schema, crs=from_epsg(4326))
    c = 0
    try:
        for tweet in tweet_set:
            if tweet.has_key('geo') and tweet['geo'].has_key('coordinates'):
                tweet['geo']['coordinates'] = [tweet['geo']['coordinates'][1], tweet['geo']['coordinates'][0]]
                timestamp = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                time = timestamp.isoformat('T')
                try:
                        tb = TextBlob(tweet['text'].lower(), pos_tagger=PerceptronTagger())
                        nb = TextBlob(tweet['text'], analyzer=NaiveBayesAnalyzer())
                        pol_tb = str(tb.polarity)
                        sub_tb = str(tb.subjectivity)
                        pol_nb = str(nb.polarity)
                        sub_nb = str(nb.subjectivity)
                        sent_p_nb = str(nb.sentiment[1])
                        sent_n_nb = str(nb.sentiment[2])
                        sent_c_nb = str(nb.sentiment[0])
                except:
                    pol_tb= sub_tb= pol_nb= sub_nb= sent_p_nb= sent_n_nb= sent_c_nb = "NULL"
                sent=None
                pol=None
                sent_type=None
                cat=None
                catg=None
                cat_sc=None
                cat_url=None
                try:
                    sent = calc_sentiment(tweet['text'])
                    pol = sent['docSentiment']['score']
                    sent_type = sent['docSentiment']['type']
                    cat = api.category('text', tweet['text'])
                    catg = cat['category']
                    cat_sc = cat['score']
                    cat_url = cat['url']

                except:
                    pass

                if cat_url is None or len(cat_url) < 1:
                    cat_url = 'NULL'
                if pol is None or len(pol) < 1:
                    pol = 'NULL'
                if cat_sc is None or len(cat_sc) < 1:
                    cat_sc = 'NULL'
                if catg is None or len(catg) < 1:
                    catg = 'NULL'
                if sent_type is None or len(sent_type) < 1:
                    sent_type = 'NULL'
                rec = {'geometry': tweet['geo'],
                       'properties': {
                           'tweet_id': str(tweet['id']),
                           'user_id': str(tweet['user']['id']),
                           'created_at': time,
                           'id': c,
                           'text': tweet['text'],
                           'pol_alc': pol,
                           'sent_alc': sent_type,
                           'category': catg,
                           'cat_conf': cat_sc,
                           'cat_url': cat_url,
                           'pol_tb': pol_tb,
                           'sub_tb': sub_tb,
                           'sent_nltk': sent_c_nb,
                           'sen_pos_n': sent_p_nb,
                           'sen_neg_n': sent_n_nb,
                           'pol_nltk': pol_nb,
                           'sub_nltk': sub_nb
                           }}
                print rec
                #print shp.validate_record(rec)
                #print shp.validate_record_geometry(rec)
                shp.write(rec)

                c += 1
    except:
        pass
    shp.close()
import threading
if __name__ == "__main__":
    from shapely import geometry
    n = 0
    jobs = []
    for rex in poly:
        try:
           p = geometry.shape(rex['geometry'])
           xy = zip(p.boundary.xy[1],p.boundary.xy[0])
           res = list(tweets_toronto.find({'geo.coordinates': {"$geoWithin": {"$polygon":xy}}}))
           p= threading.Thread(target=make_recs,args=(n,res))
           p.start()
           jobs.append(p)
           if len(jobs)>10:
               p.join()
           n+=1
        except:
            pass






