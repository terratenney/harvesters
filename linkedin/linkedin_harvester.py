__author__ = 'mtenney'
from linkedin import linkedin, linkedin_keys

RETURN_URL = 'http://localhost:8000'
import networkx as nx

from py2neo import neo4j

graph = neo4j.GraphDatabaseService()
g = nx.Graph()


authentication = linkedin.LinkedInDeveloperAuthentication(linkedin_keys.Api_Key, linkedin_keys.Api_Secret,
                                                          linkedin_keys.oAuth_Token, linkedin_keys.oAuth_Secret,
                                                          RETURN_URL, linkedin.PERMISSIONS.enums.values())



application = linkedin.LinkedInApplication(authentication)
application.search_profile(selectors='location',params={u'location': {u'country': {u'code': u'ca'}, u'name': u'Toronto, Canada'}})
g.add_node('Matthew Tenney',)

connections = application.get_connections()
for connection in connections['values']:
    n = connection['firtName']+' '+connection['lastName']
    g.add_node(n,connection)

print connections

