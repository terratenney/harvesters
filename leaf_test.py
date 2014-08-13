__author__ = 'mtenney'
import pymongo
import folium
import vincent
import pandas as pd
import fiona
import datetime
from shapely import geometry
con = pymongo.MongoClient()
db = con.tweets
tweets_toronto = db.tweets_toronto
import os
os.chdir('../')
poly = fiona.open('D:\data\open_toronto\NEIGHBORHOODS_WGS84.shp')
from collections import Counter, OrderedDict
boros = {}

c = 0
for rex in poly:
    p = geometry.shape(rex['geometry'])
    xy = zip(p.boundary.xy[1],p.boundary.xy[0])
    res = list(tweets_toronto.find({'geo.coordinates': {"$geoWithin": {"$polygon":xy}}}))
    boros[rex['properties']['AREA_NAME']] = {'tweets':res, 'times':[datetime.datetime.strptime(tweet['created_at'],
                                                                '%a %b %d %H:%M:%S +0000 %Y')for tweet in res]}
    c+=1


poly = fiona.open('D:\data\open_toronto\NEIGHBORHOODS_WGS84.shp')
map = folium.Map([43.5,-79.37],width=1250,height=900,tiles='Stamen Toner')

c = 0
for rex in poly:
    p = geometry.shape(rex['geometry'])
    loc = [p.centroid.coords.xy[1][0],p.centroid.coords.xy[0][0]]
    hours = [h.hour for h in boros[rex['properties']['AREA_NAME']]['times']]
    hours = OrderedDict(Counter(hours))
    v = vincent.Bar(hours)
    v.colors(brew='Set2')
    v.axis_titles(x='Hour of The Day',y='Number of Tweets')
    fn = str(c)
    v.to_json('hour'+fn+'.json',True,'hour'+fn+'.html')
    map.simple_marker(loc, popup=(v,'hour'+fn+'.json'))
    del(v)
    c+=1

map.create_map('boro_hour_map.html')


poly = fiona.open('D:\data\open_toronto\NEIGHBORHOODS_WGS84.shp')
map1 = folium.Map([43.5,-79.37],width=1250,height=600,tiles='Stamen Toner')


c = 0
for rex in poly:
    p = geometry.shape(rex['geometry'])
    loc = [p.centroid.coords.xy[1][0],p.centroid.coords.xy[0][0]]
    hours = [h.isoweekday() for h in boros[rex['properties']['AREA_NAME']]['times']]
    hours = OrderedDict(Counter(hours))
    v = vincent.Bar(hours)
    v.colors(brew='Set1')
    v.axis_titles(x='Day of the Week',y='Number of Tweets')
    fn = str(c)
    v.to_json('day'+fn+'.json',True,'day'+fn+'.html')
    map1.simple_marker(loc,popup=(v,'day'+fn+'.json'))
    c+=1

map1.create_map('boro_dow_map.html')


poly = fiona.open('D:\data\open_toronto\NEIGHBORHOODS_WGS84.shp')
map1 = folium.Map([43.5,-79.37],width=1250,height=600,tiles='Stamen Toner')

c = 0
for rex in poly:
    p = geometry.shape(rex['geometry'])
    loc = [p.centroid.coords.xy[1][0],p.centroid.coords.xy[0][0]]
    hours = [{'day':h.isoweekday(),'hour':h.hour} for h in boros[rex['properties']['AREA_NAME']]['times']]
    #hours = OrderedDict(Counter(hours))
    v = vincent.GroupedBar(pd.DataFrame(hours))
    v.colors(brew='Set2')
    v.axis_titles(x='Time of Day',y='Number of Tweets')
    fn = str(c)
    v.to_json('dowhr'+fn+'.json',True,'dowhr'+fn+'.html')
    map1.simple_marker(loc,popup=(v,'dowhr'+fn+'.json'))
    c+=1

map1.create_map('boro_dowhr_map.html')
