__author__ = 'mtenney'
import fiona
user_areas = {}
records = []
for f in fiona.open('D:\\data\\neartoronto.shp'):
    user_areas[f['properties']['user_id']] = []

for f in fiona.open('D:\\data\\neartoronto.shp'):
    user_areas[f['properties']['user_id']].append(f['geometry']['coordinates'])

# for k,points in user_areas.items():
#     try:
#         if len(points)> 9:
#
#             from scipy.spatial import Delaunay
#             tri = Delaunay(points)
#             edges = set()
#             edge_points = []
#             def add_edge(i, j):
#                  """Add a line between the i-th and j-th points, if not in the list already"""
#                  if (i, j) in edges or (j, i) in edges:
#                     # already added
#                     return
#                  edges.add( (i, j) )
#                  edge_points.append(points[ [i, j] ])
#             import numpy as np
#             points= np.array(points)
#             import math
#             edges = set()
#             edge_points = []
#             alpha = 0.9
#             # loop over triangles:
#             # ia, ib, ic = indices of corner points of the triangle
#             for ia, ib, ic in tri.vertices:
#                 pa = points[ia]
#                 pb = points[ib]
#                 pc = points[ic]
#
#                 # Lengths of sides of triangle
#                 a = math.sqrt((pa[0]-pb[0])**2 + (pa[1]-pb[1])**2)
#                 b = math.sqrt((pb[0]-pc[0])**2 + (pb[1]-pc[1])**2)
#                 c = math.sqrt((pc[0]-pa[0])**2 + (pc[1]-pa[1])**2)
#
#                 # Semiperimeter of triangle
#                 s = (a + b + c)/2.0
#
#                 # Area of triangle by Heron's formula
#                 area = math.sqrt(s*(s-a)*(s-b)*(s-c))
#
#                 circum_r = a*b*c/(4.0*area)
#
#                 # Here's the radius filter.
#                 if circum_r < 1.0/alpha:
#                     add_edge(ia, ib)
#                     add_edge(ib, ic)
#                     add_edge(ic, ia)
#
#                 #lines = LineCollection(edge_points)
#             from shapely.geometry import mapping
#             from shapely.geometry import MultiLineString
#             from shapely.ops import cascaded_union, polygonize
#
#             m = MultiLineString(edge_points)
#             # schema = {'geometry': 'LineString','properties': {'test': 'str'}}
#             # with fiona.open('D:/data/'+k+'_delaunay.shp','w','ESRI Shapefile', schema) as e:
#             #        e.write({'geometry':mapping(m), 'properties':{'test':'Delaunay'}})
#             triangles = list(polygonize(m))
#
#             # sauver triangles
#
#             # schema = {'geometry': 'Polygon','properties': {'test': 'str'}}
#             # with fiona.open('D:/data/'+k+'_triangles.shp','w','ESRI Shapefile', schema) as e:
#             #       for geom in triangles:
#             #           e.write({'geometry':mapping(geom), 'properties':{'test':'Triangle'}})
#             rec = {'geometry':mapping(cascaded_union(triangles)), 'properties':{'user_id':k}}
#             records.append(rec)
#
#     except:
#         pass

from matplotlib import pyplot as plot
from shapely.ops import cascaded_union, polygonize
from shapely import geometry
from scipy.spatial import Delaunay
import numpy as np
import math

from descartes import PolygonPatch

def plot_polygon(polygon):
    fig = pl.figure(figsize=(10,10))
    ax = fig.add_subplot(111)
    margin = .3

    x_min, y_min, x_max, y_max = polygon.bounds

    ax.set_xlim([x_min-margin, x_max+margin])
    ax.set_ylim([y_min-margin, y_max+margin])
    patch = PolygonPatch(polygon, fc='#999999',
                         ec='#000000', fill=True,
                         zorder=-1)
    ax.add_patch(patch)
    return fig



def alpha_shape(points, alpha):
    """
    Compute the alpha shape (concave hull) of a set
    of points.

    @param points: Iterable container of points.
    @param alpha: alpha value to influence the
        gooeyness of the border. Smaller numbers
        don't fall inward as much as larger numbers.
        Too large, and you lose everything!
    """
    if len(points) < 4:
        # When you have a triangle, there is no sense
        # in computing an alpha shape.
        return geometry.MultiPoint(points).convex_hull

    def add_edge(edges, edge_points, coords, i, j):
        """
        Add a line between the i-th and j-th points,
        if not in the list already
        """
        if (i, j) in edges or (j, i) in edges:
            # already added
            return
        edges.add( (i, j) )
        edge_points.append(coords[ [i, j] ])

    coords = np.array(points)

    tri = Delaunay(coords)
    edges = set()
    edge_points = []
    # loop over triangles:
    # ia, ib, ic = indices of corner points of the
    # triangle
    for ia, ib, ic in tri.vertices:
        pa = coords[ia]
        pb = coords[ib]
        pc = coords[ic]

        # Lengths of sides of triangle
        a = math.sqrt((pa[0]-pb[0])**2 + (pa[1]-pb[1])**2)
        b = math.sqrt((pb[0]-pc[0])**2 + (pb[1]-pc[1])**2)
        c = math.sqrt((pc[0]-pa[0])**2 + (pc[1]-pa[1])**2)

        # Semiperimeter of triangle
        s = (a + b + c)/2.0

        # Area of triangle by Heron's formula
        area = math.sqrt(s*(s-a)*(s-b)*(s-c))
        circum_r = a*b*c/(4.0*area)

        # Here's the radius filter.
        #print circum_r
        if circum_r < 1.0/alpha:
            add_edge(edges, edge_points, coords, ia, ib)
            add_edge(edges, edge_points, coords, ib, ic)
            add_edge(edges, edge_points, coords, ic, ia)

    m = geometry.MultiLineString(edge_points)
    triangles = list(polygonize(m))
    return cascaded_union(triangles), edge_points


# points = [geometry.shape(point['geometry']['coordinates'])for point in f]
import pylab as pl

for k,points in user_areas.items():
    if len(points)>200:
        concave_hull, edge_points = alpha_shape(points,alpha=2)
        _ = plot_polygon(concave_hull)
        pl.show()
        # _ = pl.plot(x,y,'o', color='#f16824')
        break


# schema = {'geometry': 'Polygon','properties': {'user_id': 'str'}}
# with fiona.open('D:/data/all_alpha_hulls.shp','w','ESRI Shapefile', schema) as e:
#     e.writerecords(records)
#     e.close()
