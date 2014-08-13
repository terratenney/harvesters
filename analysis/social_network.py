__author__ = 'mtenney'
import pymongo
import networkx as nx
from multiprocessing import Process, Queue
from joblib import Parallel, delayed
import gc
con = pymongo.MongoClient()
db = con.tweets
tweets = db.tweets_toronto
users = db.twitter_users
# g = nx.DiGraph()
user_ids = tweets.find({'geo.coordinates':{"$within": {"$center": [[44, -79], 1]}}}).distinct('user.id')
import fiona
import networkx as nx
G = nx.Graph()

for user in user_ids:
    user = users.find_one({'id':user})
    G.add_node(user['id'],user)
    if user.has_key('followers_list'):
        for follower in user['followers_list']:
            follower_rec = users.find_one({'id':follower})
            if follower_rec is not None and follower_rec != None and not (follower_rec is None):
                try:
                    G.add_node(follower_rec['id'],follower_rec)
                    G.add_edge(user['id'],follower_rec['id'],{'relationship':'Follows'})
                except:
                    pass

    if user.has_key('friends_list'):
        for friend in user['friends_list']:
            friend_rec = users.find_one({'id':friend})
            if friend_rec is not None and friend_rec != None and not (friend_rec is None):
                try:
                    G.add_node(follower_rec['id'],follower_rec)
                    G.add_edge(user['id'],follower_rec['id'],{'relationship':'Friends'})
                except:
                    pass

nx.write_gpickle(G,'Z:/')

nx.write_gpickle(G,'Z:/g.gpickle')

from multiprocessing import Pool
import time
import itertools
import networkx as nx


def chunks(l, n):
    """Divide a list of nodes `l` in `n` chunks"""
    l_c = iter(l)
    while 1:
        x = tuple(itertools.islice(l_c, n))
        if not x:
            return
        yield x


def _betmap(G_normalized_weight_sources_tuple):
    """Pool for multiprocess only accepts functions with one argument.
    This function uses a tuple as its only argument. We use a named tuple for
    python 3 compatibility, and then unpack it when we send it to
    `betweenness_centrality_source`
    """
    return nx.betweenness_centrality_source(*G_normalized_weight_sources_tuple)


def betweenness_centrality_parallel(G, processes=None):
    """Parallel betweenness centrality  function"""
    p = Pool(processes=processes)
    node_divisor = len(p._pool)*4
    node_chunks = list(chunks(G.nodes(), int(G.order()/node_divisor)))
    num_chunks = len(node_chunks)
    bt_sc = p.map(_betmap,
                  zip([G]*num_chunks,
                      [True]*num_chunks,
                      [None]*num_chunks,
                      node_chunks))

    # Reduce the partial solutions
    bt_c = bt_sc[0]
    for bt in bt_sc[1:]:
        for n in bt:
            bt_c[n] += bt[n]
    return bt_c

if __name__ == "__main__":

    user_nodes = nx.read_shp('D:\\data\\user_points.shp')
    tweet_nodes = nx.read_shp('D:\\data\\near_toronto.shp')

    g = nx.Graph()
    users = {}


    for user in user_nodes.nodes_iter(data=True):
        users[user[1]['user_id']] = user

    for tweet in tweet_nodes.nodes_iter(data=True):
        uk = tweet[1]['user_id']
        if users.has_key(uk):
            un = users[uk]
            g.add_node(un[0],un[1])
            g.add_node(tweet[0],tweet[1])
            g.add_edge(un[0],tweet[0])
        else:
            pass
        print("")
        print("Computing betweenness centrality for:")
        print(nx.info(g))
        print("\tParallel version")
        start = time.time()
        bt = betweenness_centrality_parallel(g)
        print("\t\tTime: %.4F" % (time.time()-start))
        print("\t\tBetweenness centrality for node 0: %.5f" % (bt[0]))
        print("\tNon-Parallel version")
        start = time.time()
        bt = nx.betweenness_centrality(g)
        print("\t\tTime: %.4F seconds" % (time.time()-start))
        print("\t\tBetweenness centrality for node 0: %.5f" % (bt[0]))
    print("")

#
# for feature in  fiona.open('D:\data\user_points.shp'):
#
#
#
#     if user['id'] in user_ids:
#         no_reply = []
#         g.add_node(user['id'],user)
#
#         for fid in user['followers_list']:
#             follower = users.find_one({'id':fid})
#             if follower:
#                 g.add_node(fid,follower)
#                 print 'Adding edge'
#                 g.add_edge(user['id'],fid)
#
#
# nx.write_gexf(g,'Z:/followers_list.gexf')
