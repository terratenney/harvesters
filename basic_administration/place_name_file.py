__author__ = 'mtenney'
import geonamescache

gc = geonamescache.GeonamesCache()

with open('C:\\Python27\\ArcGISx6410.2\\Lib\\site-packages\\pattern\\text\\en\\wordlist\\places.txt','a') as f:
    conts =  gc.get_continents()
    for key in conts.keys():
        f.write(' '+conts[key]['name'].encode('utf-8')+',')
    counts = gc.get_countries()
    for key in counts.keys():
        f.write(' '+counts[key]['name'].encode('utf-8')+',')
    cities = gc.get_cities()
    for key in cities.keys():
        f.write(' '+cities[key]['name'].encode('utf-8')+',')