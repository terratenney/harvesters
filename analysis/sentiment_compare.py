__author__ = 'mtenney'
import pymongo
import pattern
import textblob
from pattern.en import sentiment
from pattern.en import suggest
from pattern.en import parse, Sentence
from pattern.en import tag, parsetree
from pattern.vector import KNN, count
from pattern.vector import Document, Model, TFIDF,L2
import cPickle as pickle


con = pymongo.MongoClient()
sentiment_res = con.tweets.sentiment_analysis
sentiment_res_p = con.tweets.patterns_sentiment_analysis
tweets = con.tweets.tweets_toronto

docs = []
# with open('D:\\data\\documents.spkl', 'wb') as fp:
#     for tweet in tweets.find():
#         doc = Document(tweet['text'],name=tweet['id'])
#         pickle.dump(doc, fp)
#     fp.close()
#

m = Model(documents=[],weight=TFIDF)

with open('D:\\data\\documents.spkl', 'rb') as fp:
    for j in range(tweets.count()/100):
        print 'Loading model'
        m.append(pickle.load(fp))
        print len(m.documents)
with open('D:\\data\\documents.spkl', 'rb') as fp:
    for j in xrange(tweets.count()):
        print 'Loading model'
        m.append(pickle.load(fp))
        print len(m.documents)
    print len(m.documents)
m.reduce(dimensions=L2)
m.save
