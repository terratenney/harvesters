__author__ = 'mtenney'

from gensim import corpora, models, similarities
import nltk
from nltk import sent_tokenize, word_tokenize, pos_tag, ne_chunk
from pymongo import MongoClient
conn = MongoClient('mongodb://localhost:27017/')
db = conn.tweets
db.tweets_toronto.ensure_index('id', unique=True, drop_dups=True, sparse=True)
db.twitter_users.ensure_index('id', unique=True, drop_dups=True, sparse=True)
tweets = db.tweets_toronto
twitter_users = db.twitter_users
##############################################################################################################
import logging, gensim, bz2

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

from gensim import corpora, models, similarities
from textblob import TextBlob
from pattern.en import suggest
from pattern.en import parsetree
from textblob_aptagger import PerceptronTagger
# docs =[]
# import pattern.en
# for tweet in tweets.find().limit(1000):
#     d = TextBlob(tweet['text'].lower(), pos_tagger=PerceptronTagger())
#     #p = parsetree(tweet['text'].lower())
#     #print [word for word in p.sentences[0].tagged if word[1][0] == 'N' and word[0] not in pattern.en.wordlist.STOPWORDS +['/','@',',','.','_']]
#     d= [word[0] for word in d.tags if word[1][0] == 'N'  and word[0] not in pattern.en.wordlist.STOPWORDS+['/','@',',','.','_']]
#     #print
#     #print p.sentences[0].subjects
#     docs.append(d)
# dictionary = corpora.Dictionary(docs)
# corpus = [dictionary.doc2bow(doc) for doc in docs]
# del(docs)
# import gc
# gc.collect()
corpus= corpora.MmCorpus.load('./tweetsl.mm')
dictionary = corpora.Dictionary.load('./tweetsl.dict')

lsi = gensim.models.lsimodel.LsiModel(corpus=corpus, id2word=dictionary, num_topics=40, chunksize=20, distributed=True)

lda = gensim.models.ldamodel.LdaModel(corpus=corpus, num_topics=40, id2word=dictionary, distributed=True, chunksize=20, passes=1, update_every=1, alpha='symmetric', eta=None, decay=0.5, eval_every=10, iterations=50, gamma_threshold=0.001)

import cPickle as pickle

pickle.dump(lsi,open('./lsil.spkl','wb'),pickle.HIGHEST_PROTOCOL)
pickle.dump(lda,open('./ldal.spkl','wb'),pickle.HIGHEST_PROTOCOL)
