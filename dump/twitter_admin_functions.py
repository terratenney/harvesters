__author__ = 'mtenney'

from dump.rate_limit_regulator import *

def get_rate_limits_status(api):
    limits = api.GetRateLimitStatus()['resources']
    max_call = limits['statuses']['/statuses/user_timeline']['limit']
    remaining_call = limits['statuses']['/statuses/user_timeline']['remaining']
    reset_time = limits['statuses']['/statuses/user_timeline']['reset']
    time_to_reset = abs(reset_time - time.time())
    return [max_call, remaining_call, time_to_reset]



