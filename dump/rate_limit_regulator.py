__author__ = 'mtenney'
import time

def RateLimited(maxPerMinute):
    minInterval = 60.0 / float(maxPerMinute)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate



def rate_limited(max_per_second):
    import threading
    '''
    Decorator that make functions not be called faster than
    '''
    lock = threading.Lock()
    minInterval = 60.0 / float(max_per_second)

    def decorate(func):
        lastTimeCalled = [0.0]

        def rateLimitedFunction(*args, **kargs):
            lock.acquire()
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                time.sleep(leftToWait)
            lock.release()
            ret = func(*args, **kargs)
            lastTimeCalled[0] = time.clock()
            return ret

        return rateLimitedFunction

    return decorate

@rate_limited(23)
def print_func(num):
    print num

def run():
    for i in range(30):
        print_func(i)

import threading
if __name__ == "__main__":
    threading.Thread(target=run).start()

