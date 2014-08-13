from __future__ import division

__author__ = 'mtenney'

from collections import defaultdict
from pylab import barh, show, yticks
import collections
from pymongo import MongoClient
import sys
import nltk
import re
import pymongo

def plot_histogram(freq, mean):
    # using dict comprehensions to remove not frequent words

    topwords = {word: count
                for word, count in freq.items()
                if count > round(8 * mean)}
    sorted_alpha = collections.OrderedDict(sorted(topwords.items()))
    # plotting
    y = sorted_alpha.values()
    x = range(len(y))

    labels = topwords.keys()
    barh(x, y, align='center')
    yticks(x, labels)
    show()


class TwitterAnalyzer:
    def __init__(self):
        self.freq = defaultdict(int)
        self.cnt = 0
        self.mean = 0.0
        self.con = MongoClient()
        self.tweet_recs = self.con.tweets.tweets_toronto


    def on_receive(self, tweet):
        """ Handles the arrive of a single tweet """
        self.cnt += 1
        # a little bit of natural language processing
        tokens = nltk.word_tokenize(tweet)  # tokenize
        tagged_sent = nltk.pos_tag(tokens)  # Part Of Speech tagging
        for word, tag in tagged_sent:
            # filter sigle chars words and symbols
            if len(word) > 1 and re.match('[A-Za-z0-9 ]+', word):
                # consider only adjectives and nouns
                if tag == 'JJ' or tag == 'NN':
                    self.freq[word] += 1  # keep the count
        # print the statistics every 50 tweets
        if self.cnt % 50 == 0:
            self.print_stats()

    def print_stats(self):
        maxc = 0
        sumc = 0
        for word, count in self.freq.items():
            if maxc < count:
                maxc = count
            sumc += count
        self.mean = sumc / len(self.freq)
        print '-------------------------------'
        print ' tweets analyzed:', self.cnt
        print ' words extracted:', len(self.freq)
        print '   max frequency:', maxc
        print '  mean frequency:', self.mean

    def close_and_plot(self):
        print ' Plotting...'
        plot_histogram(self.freq, self.mean)
        sys.exit(0)

    def save_file(self,out_file):
        with open(out_file,'w') as f:
            for item in self.freq:
                f.write(str(item)+"\n")
            f.close()



if __name__ == '__main__':
    an = TwitterAnalyzer()
    con = pymongo.MongoClient()
    db = con.tweets
    print db.tweets_toronto.count()
    for tweet in db.tweets_toronto.find():
        text=unicode(tweet['text'])
        an.on_receive(text)
    an.save_file('D:\data\word_freq.txt')
    an.close_and_plot()
