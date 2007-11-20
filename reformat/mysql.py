# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader mysql reformating module
#
from pgloader.tools import PGLoader_Error

def timestamp(reject, input):
    """ Reformat str as a PostgreSQL timestamp

    MySQL timestamps are like:  20041002152952
    We want instead this input: 2004-10-02 15:29:52
    """
    if len(input) != 14:
        e = "MySQL timestamp reformat input too short: %s" % input
        raise PGLoader_Error, e
    
    year    = input[0:4]
    month   = input[4:6]
    day     = input[6:8]
    hour    = input[8:10]
    minute  = input[10:12]
    seconds = input[12:14]
    
    return '%s-%s-%s %s:%s:%s' % (year, month, day, hour, month, seconds)
