__author__ = 'mtenney'
import datetime
import time
import collections

import pymongo
import threading

con = pymongo.MongoClient()
tweets = con.tweets.tweets_toronto
dates = []
import os, csv, re

from optparse import OptionParser

import datetime, pytz
from dateutil.tz import tzlocal

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.patches as patches
import matplotlib.path as path
import matplotlib.cm as cm

from collections import Counter

from nltk.cluster import KMeansClusterer, GAAClusterer, euclidean_distance
import nltk.corpus
from nltk import decorators
import nltk.stem

# HEADER = [  'tweet_id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweeted_status_id', \
# 'retweeted_status_user_id', 'created_at', 'source', 'text', 'expanded_urls']
# HEADER_DICT = dict( (name,i) for i, name in enumerate(HEADER) )

stemmer_func = nltk.stem.snowball.EnglishStemmer().stem
stopwords = set(nltk.corpus.stopwords.words('english'))



def by_hour():
    hours = []
    for tweet in tweets.find():
        timestamp_str = tweet['created_at']
        #timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S +0000')
        timestamp = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
        timestamp = timestamp.replace(tzinfo=pytz.utc)
        timestamp = timestamp.astimezone(tzlocal())
        hours.append(timestamp.hour)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    n, bins = np.histogram(hours, range(25))

    print n, bins

    # get the corners of the rectangles for the histogram
    left = np.array(bins[:-1])
    right = np.array(bins[1:])
    bottom = np.zeros(len(left))
    top = bottom + n

    # we need a (numrects x numsides x 2) numpy array for the path helper
    # function to build a compound path
    XY = np.array([[left, left, right, right], [bottom, top, top, bottom]]).T

    # get the Path object
    barpath = path.Path.make_compound_path_from_polys(XY)

    # make a patch out of it
    patch = patches.PathPatch(barpath, facecolor='blue', edgecolor='gray', alpha=0.8)
    ax.add_patch(patch)

    # update the view limits
    ax.set_xlim(left[0], right[-1])
    ax.set_ylim(bottom.min(), top.max())

    #plt.ticklabel_format(style='sci', axis='x', scilimits=(0,0))
    plt.xticks(range(0, 24), ha='center')

    plt.xlabel('Hour')
    plt.ylabel('# Tweets')
    plt.title('# of Tweets by Hour')

    plt.savefig('by-hour.png', bbox_inches=0)
    plt.show()


def by_dow():
    dow = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    c = Counter()
    for tweet in tweets.find():
        timestamp_str = tweet['created_at']
        #timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S +0000')
        timestamp = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
        timestamp = timestamp.replace(tzinfo=pytz.utc)
        timestamp = timestamp.astimezone(tzlocal())
        c[timestamp.strftime('%A')] += 1
    print c.most_common(10)

    N = len(dow)

    ind = np.arange(N)
    width = 0.9

    fig = plt.figure()
    ax = fig.add_subplot(111)

    rects1 = ax.bar(0.05 + ind, [c[d] for d in dow], width, color='b')

    ax.set_ylabel('# Tweets')
    ax.set_title('Tweets by Day of Week')
    ax.set_xticks(ind + 0.5 * width)
    ax.set_xticklabels([d[:3] for d in dow])

    plt.savefig('by-dow.png', bbox_inches=0)
    plt.show()


# def by_month():
#     c = Counter()
#     for tweet in tweets.find():
#         timestamp_str = tweet['created_at']
#        #timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S +0000')
#         timestamp = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
#         timestamp = timestamp.replace(tzinfo=pytz.utc)
#         timestamp = timestamp.astimezone(tzlocal())
#         c[timestamp.strftime('%Y-%m')] += 1
#     print c.most_common(10)

    N = len(c)

    ind = np.arange(N)  # the x locations for the groups
    width = 0.8  # the width of the bars

    fig = plt.figure()
    ax = fig.add_subplot(111)

    rects1 = ax.bar(ind, [c[x] for x in sorted(c.keys())], width, color='b')

    ax.set_ylabel('# Tweets')
    ax.set_title('Tweets by Month')

    ax.set_xticks([i for i, x in enumerate(sorted(c.keys())) if i % 6 == 0])
    ax.set_xticklabels([x for i, x in enumerate(sorted(c.keys())) if i % 6 == 0], rotation=30)

    plt.savefig('by-month.png', bbox_inches=0)
    plt.show()


def by_month_dow():
    dow = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    # Get the # of week and weekday for each tweet
    data = {}
    for tweet in tweets.find():
        timestamp_str = tweet['created_at']
        #timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S +0000')
        timestamp = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
        timestamp = timestamp.replace(tzinfo=pytz.utc)
        timestamp = timestamp.astimezone(tzlocal())
        weekday = timestamp.strftime('%A')
        iso_yr, iso_wk, iso_wkday = timestamp.isocalendar()
        #key = str(iso_yr) + '-' + str(iso_wk)
        key = timestamp.strftime('%Y-%m')
        if key not in data:
            data[key] = Counter()
        data[key][weekday] += 1
    print data
    # Convert to numpy
    xs = []
    ys = []
    a = np.zeros((7, len(data)))
    for i, key in enumerate(sorted(data.iterkeys())):
        for j, d in enumerate(dow):
            #a[j,i] = data[key][d]
            for k in range(data[key][d]):
                xs.append(j)
                ys.append(i)
    # Convert to x,y pairs
    #  heatmap, xedges, yedges = np.histogram2d(np.array(xs), np.array(ys), bins=(7,len(data)))
    #  extent = [xedges[0], xedges[-1], yedges[0], yedges[-1]]
    #  plt.clf()
    #  plt.imshow(heatmap, extent=extent)
    #  plt.show()

    x = np.array(xs)
    y = np.array(ys)

    gridsize = 30
    plt.hexbin(x, y, C=None, gridsize=gridsize, cmap=cm.jet, bins=None)
    plt.axis([x.min(), x.max(), y.min(), y.max()])
    plt.title('Tweets by Day of Week and Month')
    plt.xlabel('Day of Week')
    plt.ylabel('Month')
    plt.gca().set_xticklabels([d[:3] for d in dow])
    plt.gca().set_yticklabels([key for i, key in enumerate(sorted(data.iterkeys())) if i % 6 == 0])
    plt.gca().set_yticks([i for i, key in enumerate(sorted(data.iterkeys())) if i % 6 == 0])
    print [key for key in sorted(data.iterkeys())]
    cb = plt.colorbar()
    cb.set_label('# Tweets')
    plt.savefig('by-month-dow.png', bbox_inches=0)
    plt.show()


def by_month_length():
    c = Counter()
    s = Counter()
    for tweet in tweets.find():
        timestamp_str = tweet['created_at']
        #timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S +0000')
        timestamp = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
        timestamp = timestamp.replace(tzinfo=pytz.utc)
        timestamp = timestamp.astimezone(tzlocal())
        c[timestamp.strftime('%Y-%m')] += 1
        s[timestamp.strftime('%Y-%m')] += len(tweet['text'])
    print c.most_common(10)

    N = len(c)
    ind = np.arange(N)
    width = 0.8

    fig = plt.figure()
    ax = fig.add_subplot(111)

    rects1 = ax.bar(ind, [s[x] / c[x] for x in sorted(c.keys())], width, color='b')

    ax.set_ylabel('Avg Tweet Length')
    ax.set_title('Avg Tweet Length by Month')

    ax.set_xticks([i for i, x in enumerate(sorted(c.keys())) if i % 6 == 0])
    ax.set_xticklabels([x for i, x in enumerate(sorted(c.keys())) if i % 6 == 0], rotation=30)

    plt.savefig('by-month-length.png', bbox_inches=0)
    plt.show()


def by_month_type():
    c_total = Counter()
    c_tweets = Counter()
    c_rts = Counter()
    c_replies = Counter()
    months = set()
    for tweet in tweets.find():
        timestamp_str = tweet['created_at']
        #timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S +0000')
        timestamp = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
        timestamp = timestamp.replace(tzinfo=pytz.utc)
        timestamp = timestamp.astimezone(tzlocal())
        key = timestamp.strftime('%Y-%m')
        months.add(key)
        c_total[key] += 1
        # if tweet['in_reply_to_status_id']:
        #     c_replies[key] += 1
        # elif tweet['retweeted_status_id']:
        #     c_rts[key] += 1
        # else:
        #     c_tweets[key] += 1

    months = [x for x in sorted(months)]
    N = len(months)
    ind = np.arange(N)

    # Create the non stacked version
    width = 0.3

    fig = plt.figure()
    ax = fig.add_subplot(111)

    rects1 = ax.bar(ind, [c_tweets[m] for m in months], width, color='r')
    rects2 = ax.bar(ind + width, [c_rts[m] for m in months], width, color='b')
    rects3 = ax.bar(ind + width * 2, [c_replies[m] for m in months], width, color='g')

    ax.set_ylabel('# Tweets')
    ax.set_title('Type of Tweet by Month')

    ax.set_xticks([i + width for i, x in enumerate(months) if i % 6 == 0])
    ax.set_xticklabels([x for i, x in enumerate(months) if i % 6 == 0], rotation=30)

    ax.legend((rects1[0], rects2[0], rects3[0]), ('Tweet', 'RT', 'Reply'))

    fig.set_size_inches(12, 6)
    plt.savefig('by-month-type.png', bbox_inches=0)
    plt.show()

    # Create the stacked version
    width = 0.9

    fig = plt.figure()
    ax = fig.add_subplot(111)

    d_tweets = np.array([float(c_tweets[m]) / c_total[m] for m in months])
    d_rts = np.array([float(c_rts[m]) / c_total[m] for m in months])
    d_replies = np.array([float(c_replies[m]) / c_total[m] for m in months])

    rects1 = ax.bar(ind + width / 2, d_tweets, width, color='r')
    rects2 = ax.bar(ind + width / 2, d_rts, width, bottom=d_tweets, color='b')
    rects3 = ax.bar(ind + width / 2, d_replies, width, bottom=d_tweets + d_rts, color='g')

    ax.set_ylabel('Tweet Type %')
    ax.set_title('Type of Tweet by Month')

    ax.set_xticks([i for i, x in enumerate(months) if i % 6 == 0])
    ax.set_xticklabels([x for i, x in enumerate(months) if i % 6 == 0], rotation=30)

    ax.legend((rects1[0], rects2[0], rects3[0]), ('Tweet', 'RT', 'Reply'), loc=4)

    plt.savefig('by-month-type-stacked.png', bbox_inches=0)
    plt.show()


@decorators.memoize
def get_words(tweet_text):
    return [word.lower() for word in re.findall('\w+', tweet_text) if len(word) > 3]


def word_frequency():
    c = Counter()
    hash_c = Counter()
    at_c = Counter()
    for tweet in tweets.find():
        for word in get_words(tweet['text']):
            c[word] += 1
        for word in re.findall('@\w+', tweet['text']):
            at_c[word.lower()] += 1
        for word in re.findall('\#[\d\w]+', tweet['text']):
            hash_c[word.lower()] += 1
    print c.most_common(50)
    print hash_c.most_common(50)
    print at_c.most_common(50)


@decorators.memoize
def normalize_word(word):
    return stemmer_func(word.lower())


@decorators.memoize
def vectorspaced(tweet_text, all_words):
    components = [normalize_word(word) for word in get_words(tweet_text)]
    return np.array([word in components and not word in stopwords
                        for word in all_words], np.short)


def get_word_clusters():
    all_words = set()
    for tweet in tweets.find():
        for word in get_words(tweet['text']):
            all_words.add(word)
    all_words = tuple(all_words)

    cluster = GAAClusterer(5)
    cluster.cluster([vectorspaced(tweet['text'], all_words) for tweet in tweets.find()])

    classified_examples = [
        cluster.classify(vectorspaced(tweet['text'], all_words)) for tweet in tweets.find()
    ]

    for cluster_id, title in sorted(zip(classified_examples, job_titles)):
        print cluster_id, title


if __name__ == '__main__':
    # parser = OptionParser()
    # parser.add_option("-d", "--dir", dest="directory",
    #                   help="Twitter archive directory - FILE", metavar="FILE")
    #
    # (options, args) = parser.parse_args()

    # t1 = threading.Thread(target=by_month,args=(), name= "by_month").start()
    t2 = threading.Thread(target=by_month_type,args=(), name= "by_month_type").start()
    t3 = threading.Thread(target=by_month_length,args=(), name= "by_month_length").start()
    t4 = threading.Thread(target=by_month_dow, args=(), name= "by_month_dow").start()
    t5 = threading.Thread(target=by_dow, args=(), name= "by_dow").start()
    t6 = threading.Thread(target=by_hour, args=(), name= "by_hour").start()
    t7 = threading.Thread(target=word_frequency, args=(), name= "word_frequency").start()

    # print t1.isAlive()
    print t6.isAlive()
    print t7.isAlive()