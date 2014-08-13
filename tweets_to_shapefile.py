__author__ = 'mtenney'
from pymongo import MongoClient
# ##############################################################################################################
conn = MongoClient('mongodb://localhost:27017/')
db = conn.tweets
db.tweets_toronto.ensure_index('id', unique=True, drop_dups=True, sparse=True)
tweets_toronto = db.tweets_toronto
import fiona
import datetime
from fiona.crs import from_epsg

from textblob import TextBlob
from textblob_aptagger import PerceptronTagger
from textblob.sentiments import NaiveBayesAnalyzer
from alchemyapi_python import alchemyapi
api = alchemyapi.AlchemyAPI()


def find_entities(text):
     res = api.entities('text', text,
                        options={'disambiguate': 1, 'linkedData': 1, 'coreference': 1, 'quotations': 1, 'sentiment': 1,
                                 'maxRetrieve': 200})

     try:
         res.pop('usage')
     except:
         pass
     return res


#@rate_limited(70)
def calc_sentiment(text):
    res = api.sentiment('text', text)
    try:
        res.pop('usage')
    except:
        pass
    return res
#
#
#@rate_limited(70)
def find_concepts(text):
    res = api.concepts('text', text, options={'maxRetrieve': 1000})
    try:
        res.pop('usage')
    except:
        pass
    return res
#
#
def find_category(text):
    res = api.category('text', text, options={'showSourceText': 1})
    try:
        res.pop('usage')
    except:
        pass
    return res



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


def make_recs(tweet_set):
    records = []
    c = 0
    try:
        for tweet in tweet_set:
            if tweet.has_key('geo') and tweet['geo'].has_key('coordinates'):
                tweet['geo']['coordinates'] = [tweet['geo']['coordinates'][1], tweet['geo']['coordinates'][0]]
                timestamp = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                time = timestamp.isoformat('T')
                #pol_tb, sub_tb, pol_nb, sub_nb, sent_p_nb, sent_n_nb, sent_c_nb =  cal_sentiment(tweet['text'])
                #pol_tb= sub_tb= pol_nb= sub_nb= sent_p_nb= sent_n_nb= sent_c_nb = "NULL"
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
                           'pol_tb': tweet['pol_tb'],
                           'sub_tb': tweet['sub_tb'],
                           'sent_nltk': tweet['sent_nltk'],
                           'sen_pos_n': tweet['sen_pos_n'],
                           'sen_neg_n': tweet['sen_neg_n'],
                           'pol_nltk': tweet['pol_nltk'],
                           'sub_nltk': tweet['sub_nltk']
                           }}
                print rec
                #print shp.validate_record(rec)
                #print shp.validate_record_geometry(rec)
                records.append(rec)

                c += 1
    except:
        pass
    return records
    #q.put(records)




if __name__ == "__main__":
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

    shp = fiona.open("D:\\data\\sent_tweets.shp", 'w', 'ESRI Shapefile', schema, crs=from_epsg(4326))
    tweets = t = tweets_toronto.find({'pol_tb':{'$exists':True}})# tweets_toronto.find({'geo.coordinates': {"$within": {"$center": [[43.7, -79.4], 1]}}})
    # from multiprocessing import Process,Queue
    # more = True
    # q = Queue()
    # records = []
    # chunk = []
    # jobs = []
    # for tweet in tweets:
    #     if tweet:
    #
    #             print "Starting Process"
    #             p = Process(target=make_recs,args=(chunk,q))
    #             p.start()
    #             jobs.append(p)
    #             chunk =[]
    #
    #
    #
    # for j in jobs:
    #     print "Getting Results"
    #     records.extend(q.get())

    records = make_recs(tweets)
    print "Wrint Records:",len(records)
    shp.writerecords(records)
    shp.close()