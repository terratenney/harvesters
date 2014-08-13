__author__ = 'mtenney'

from time import gmtime,strftime
import time
#
# def update_user_modified_time(user):
#     current_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
#     if user.has_key('last_update'):
#         current_time = time.strftime('%a %b %d %H:%M:%S +0000 %Y', time.strptime(current_time, '%Y-%m-%d %H:%M:%S'))
#         user['last_update'] = current_time
#     else:
#         current_time = time.strftime('%a %b %d %H:%M:%S +0000 %Y', time.strptime(current_time, '%Y-%m-%d %H:%M:%S'))
#         user['last_update'] = current_time
#         print "Changing last update time"
#     return user