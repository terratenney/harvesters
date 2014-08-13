__author__ = 'mtenney'
import numpy


# search patterns for features
testFeatures = \
    [('hasAddict',     (' addict',)), \
     ('hasAwesome',    ('awesome',)), \
     ('hasBroken',     ('broke',)), \
     ('hasBad',        (' bad',)), \
     ('hasBug',        (' bug',)), \
     ('hasCant',       ('cant','can\'t')), \
     ('hasCrash',      ('crash',)), \
     ('hasCool',       ('cool',)), \
     ('hasDifficult',  ('difficult',)), \
     ('hasDisaster',   ('disaster',)), \
     ('hasDown',       (' down',)), \
     ('hasDont',       ('dont','don\'t','do not','does not','doesn\'t')), \
     ('hasEasy',       (' easy',)), \
     ('hasExclaim',    ('!',)), \
     ('hasExcite',     (' excite',)), \
     ('hasExpense',    ('expense','expensive')), \
     ('hasFail',       (' fail',)), \
     ('hasFast',       (' fast',)), \
     ('hasFix',        (' fix',)), \
     ('hasFree',       (' free',)), \
     ('hasFrowny',     (':(', '):')), \
     ('hasFuck',       ('fuck',)), \
     ('hasGood',       ('good','great')), \
     ('hasHappy',      (' happy',' happi')), \
     ('hasHate',       ('hate',)), \
     ('hasHeart',      ('heart', '<3')), \
     ('hasIssue',      (' issue',)), \
     ('hasIncredible', ('incredible',)), \
     ('hasInterest',   ('interest',)), \
     ('hasLike',       (' like',)), \
     ('hasLol',        (' lol',)), \
     ('hasLove',       ('love','loving')), \
     ('hasLose',       (' lose',)), \
     ('hasNeat',       ('neat',)), \
     ('hasNever',      (' never',)), \
     ('hasNice',       (' nice',)), \
     ('hasPoor',       ('poor',)), \
     ('hasPerfect',    ('perfect',)), \
     ('hasPlease',     ('please',)), \
     ('hasSerious',    ('serious',)), \
     ('hasShit',       ('shit',)), \
     ('hasSlow',       (' slow',)), \
     ('hasSmiley',     (':)', ':D', '(:')), \
     ('hasSuck',       ('suck',)), \
     ('hasTerrible',   ('terrible',)), \
     ('hasThanks',     ('thank',)), \
     ('hasTrouble',    ('trouble',)), \
     ('hasUnhappy',    ('unhapp',)), \
     ('hasWin',        (' win ','winner','winning')), \
     ('hasWinky',      (';)',)), \
     ('hasWow',        ('wow','omg')) ]

import mdp, numpy
import pdb
#######################################################################################################################

def tweet_pca_reduce( tweets_train, tweets_test, output_dim ):

    # convert dictionary feature vecs to numpy array
    print '--> Converting dictionaries to NumPy arrays'
    train_arr = numpy.array( [tweet_dict_to_nparr(t) for \
                              (t,s) in tweets_train])

    test_arr = numpy.array( [tweet_dict_to_nparr(t) for \
                             (t,s) in tweets_test])


    # compute principle components over training set
    print '--> Computing PCT'
    pca_array = mdp.pca( train_arr.transpose(), \
                         svd=True, output_dim=output_dim )


    # both train and test sets to PC space
    print '--> Projecting feature vectors to PC space'

    train_arr = numpy.dot( train_arr, pca_array )
    test_arr  = numpy.dot( test_arr,  pca_array )


    # convert projected vecs back to reduced dictionaries
    print '--> Converting NumPy arrays to dictionaries'

    reduced_train = \
        zip( [tweet_nparr_to_dict(v) for v in train_arr], \
             [s for (t,s) in tweets_train] )

    reduced_test  = \
        zip( [tweet_nparr_to_dict(v) for v in test_arr], \
             [s for (t,s) in tweets_test])

    return (reduced_train, reduced_test)


def make_tweet_nparr( txt ):
    """
    Extract tweet feature vector as NumPy array.
    """
    # result storage
    fvec = numpy.empty( len(testFeatures) )

    # search for each feature
    txtLow = ' ' + txt.lower() + ' '
    for i in range( 0, len(testFeatures) ):

        key = testFeatures[i][0]

        fvec[i] = False
        for tstr in testFeatures[i][1]:
            fvec[i] = fvec[i] or (txtLow.find(tstr) != -1)

    return fvec


def make_tweet_dict( txt ):
    """
    Extract tweet feature vector as dictionary.
    """
    txtLow = ' ' + txt.lower() + ' '

    # result storage
    fvec = {}

    # search for each feature
    for test in testFeatures:

        key = test[0]

        fvec[key] = False;
        for tstr in test[1]:
            fvec[key] = fvec[key] or (txtLow.find(tstr) != -1)

    return fvec


def tweet_dict_to_nparr( dict ):
    """
    Convert dictionary feature vector to numpy array
    """
    fvec = numpy.empty( len(testFeatures) )

    for i in range( 0, len(testFeatures) ):
        fvec[i] = dict[ testFeatures[i][0] ]

    return fvec


def tweet_nparr_to_dict( nparr, use_standard_features=False ):
    """
    Convert NumPy array to dictionary
    """
    fvec = {}

    if use_standard_features:
        assert len(nparr) == len(testFeatures)
        fvec = {}
        for i in range( 0, len(nparr) ):
            fvec[ testFeatures[i][0] ] = nparr[i]

    else:
        for i in range( 0, len(nparr) ):
            fvec[ str(i) ] = nparr[i]

    return fvec


def is_zero_dict( dict ):
    """
    Identifies empty feature vectors
    """
    has_any_features = False
    for key in dict:
        has_any_features = has_any_features or dict[key]

    return not has_any_features
########################################################################################################################
import csv, random
import nltk

# read all tweets and labels
fp = open('D:\\data\\Sentiment-Analysis-Dataset\\sentiment.csv', 'rb' )
reader = csv.reader( fp, delimiter=',', quotechar='"', escapechar='\\' )
tweets = []
for row in reader:
    if row[1] == 1:
        c = 'positive'
    else:
        c = 'negative'
    txt = row[3]
    tweets.append((c,txt))


# treat neutral and irrelevant the same
for t in tweets:
    if t[1] == 'irrelevant':
        t[1] = 'neutral'

# split in to training and test sets
random.shuffle( tweets );

fvecs = [(make_tweet_dict(t),s) for (t,s) in tweets]
v_train = fvecs[:2500]
v_test  = fvecs[2500:]


# dump tweets which our feature selector found nothing
#for i in range(0,len(tweets)):
#    if tweet_features.is_zero_dict( fvecs[i][0] ):
#        print tweets[i][1] + ': ' + tweets[i][0]


# apply PCA reduction
(v_train, v_test) =  tweet_pca_reduce( v_train, v_test, output_dim=1.0 )


# train classifier
#classifier = nltk.NaiveBayesClassifier.train(v_train);
classifier = nltk.classify.maxent.train_maxent_classifier_with_gis(v_train);


# classify and dump results for interpretation
print '\nAccuracy %f\n' % nltk.classify.accuracy(classifier, v_test)
print classifier.show_most_informative_features(200)


# build confusion matrix over test set
test_truth   = [s for (t,s) in v_test]
test_predict = [classifier.classify(t) for (t,s) in v_test]

print 'Confusion Matrix'
print nltk.ConfusionMatrix( test_truth, test_predict )
import cPickle as pickle

pickle.dump(classifier,open('./classifier_max_extent.spkl','wb'),pickle.HIGHEST_PROTOCOL)