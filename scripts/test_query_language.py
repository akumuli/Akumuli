from __future__ import print_function
import os
import sys
import socket
import datetime
import time
import akumulid_test_tools as att
import json
try:
    from urllib2 import urlopen
except ImportError:
    from urllib import urlopen

def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    return datetime.datetime.strptime(ts, "%Y%m%dT%H%M%S.%f")

class TCPChan:
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port))

    def send(self, data):
        self.__sock.send(data)

def main(path):
    if not os.path.exists(path):
        print("Path {0} doesn't exists".format(path))
        sys.exit(1)

    akumulid = att.Akumulid(path)
    # delete database
    akumulid.delete_database()
    # create empty database
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    try:
        host = '127.0.0.1'
        tcpport = 8282
        httpport = 8181

        chan = TCPChan(host, tcpport)

        # fill data in
        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 100
        print("Sending {0} messages through TCP...".format(nmsgs))
        for it in att.generate_messages(dt, delta, nmsgs, 'temp', tag='test'):
            chan.send(it)

        # read data back
        begin = dt + delta*nmsgs
        end = dt
        query = att.makequery(begin, end)
        queryurl = "http://{0}:{1}".format(host, httpport)
        response = urlopen(queryurl, json.dumps(query)).read()
        print(query)
        print(response)

    except Exception as err:
        print(err)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
