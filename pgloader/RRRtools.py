# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader librairies

import collections

class RRReader(collections.deque):
    """ Round Robin reader, which are collections.deque with a
    readlines() method"""

    def readlines(self):
        """ return next line from queue """
        while 1:
            try:
                yield self.popleft()
            except IndexError:
                return
 
