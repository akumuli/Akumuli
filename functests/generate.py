from __future__ import print_function
import os
import sys
import socket
import datetime
import time
import akumulid_test_tools as att
import json
try:
    import urllib2 as urllib
except ImportError:
    import urllib
import traceback

class TCPChan:
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port))

    def send(self, data):
        self.__sock.send(data)

def main(nmsgs):
    # fill data in
    host = '127.0.0.1'
    tcpport = 8282
    chan = TCPChan(host, tcpport)
    dt = datetime.datetime.utcnow()
    delta = datetime.timedelta(milliseconds=1)
    print("Sending {0} messages through TCP...".format(nmsgs))
    for it in att.generate_messages(dt, delta, nmsgs, 'temp', tag='test'):
        chan.send(it)
    print("Starting date-time: {0}".format(dt.strftime("%Y%m%dT%H%M%S.%f")))

if __name__ == '__main__':
    nmsgs = 1000000  # 1M is a default
    if len(sys.argv) == 2:
        nmsgs = int(sys.argv[1])
    main(nmsgs)
else:
    raise ImportError("This module shouldn't be imported")
