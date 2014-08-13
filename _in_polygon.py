__author__ = 'mtenney'
import fiona
import pandas as pd


pts = fiona.open('D:\\data\\tweets_neighborhoods.ssp')
polys = fiona.open('D:\\data\\NEIGHBORHOODS_WGS84')