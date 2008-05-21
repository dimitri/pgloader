# Author: Dimitri Fontaine <dim@tapoueh.org>
#
# pgloader time-related reformating module
#

def time(reject, input):
    """ Reformat str as a PostgreSQL time

    Input time like: 08231560
    We want instead this input: 08:23:15.60
    """
    if len(input) != 8:
        e = "time reformat input too short: %s" % input
        reject.log(e, input)
    
    hour       = input[0:2]
    minute     = input[2:4]
    seconds    = input[4:6]
    hundredths = input[6:8]
    
    return '%s:%s:%s.%s' % (hour, minute, seconds, hundredths)
