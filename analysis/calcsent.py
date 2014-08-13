__author__ = 'mtenney'
import pattern.en
from textblob import TextBlob
import pymongo
con = pymongo.MongoClient()
db = con.tweets
toronto_tweets = db.tweets_toronto
import fiona
from shapely import geometry
from textblob_aptagger import PerceptronTagger
from textblob.sentiments import NaiveBayesAnalyzer
poly = fiona.open('D:\data\open_toronto\NEIGHBORHOODS_WGS84.shp')
#
for rex in poly:
    p = geometry.shape(rex['geometry'])
    xy = zip(p.boundary.xy[1],p.boundary.xy[0])
    users = toronto_tweets.find({'geo.coordinates': {"$near": xy[4]}}).distinct('user.id')
    for user_id in users:
        for tweet in toronto_tweets.find({'user.id':user_id}):

            if not tweet.has_key('pol_tb'):
                tb = TextBlob(tweet['text'].lower(), pos_tagger=PerceptronTagger())
                nb = TextBlob(tweet['text'], analyzer=NaiveBayesAnalyzer())
                pol_tb = tb.polarity
                sub_tb = tb.subjectivity
                pol_nb = nb.polarity
                sub_nb = nb.subjectivity
                sent_p_nb = nb.sentiment[1]
                sent_n_nb = nb.sentiment[2]
                sent_c_nb = nb.sentiment[0]
                tweet['pol_tb']= pol_tb
                tweet['sub_tb']= sub_tb
                tweet['sent_nltk']= sent_c_nb
                tweet['sen_pos_n']= sent_p_nb
                tweet['sen_neg_n']= sent_n_nb
                tweet['pol_nltk']= pol_nb
                tweet['sub_nltk']= sub_nb
                print tweet
                toronto_tweets.update({'id':tweet['id']},tweet)
