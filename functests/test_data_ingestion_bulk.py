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

class UDPChan:
    def __init__(self, host, port):
        self.__host = host
        self.__port = port
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    def send(self, data):
        self.__sock.sendto(data, (self.__host, self.__port))

class TCPChan:
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port))

    def send(self, data):
        self.__sock.send(data)

def main(path, protocol):
    akumulid = att.create_akumulid(path)
    # delete database
    akumulid.delete_database()
    # create empty database
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    try:
        # fill data in
        host = '127.0.0.1'
        udpport = 8383
        tcpport = 8282
        if protocol == 'TCP':
            chan = TCPChan(host, tcpport)
        elif protocol == 'UDP':
            chan = UDPChan(host, udpport)
        else:
            print('Unknown protocol "{0}"'.format(protocol))
        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 1000000
        print("Sending {0} messages through {1}...".format(nmsgs, protocol))
        for it in att.generate_bulk_messages(dt, delta, nmsgs, ['foo', 'bar', 'buz'], tag='test'):
            chan.send(it)

        # check stats
        httpport = 8181
        statsurl = "http://{0}:{1}/api/stats".format(host, httpport)
        rawstats = urllib.urlopen(statsurl).read()
        stats = json.loads(rawstats)

        # some space should be used
        volume0space = stats["volume_0"]["free_space"]
        if "volume_1" in stats:
            volume1space = stats["volume_1"]["free_space"]
        else:
            volume1space = 0
        if volume0space == volume1space:
            print("Test #1 failed. Nothing was written to disk, /stats:")
            print(rawstats)
            sys.exit(10)
        else:
            print("Test #1 passed")
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
else:
    raise ImportError("This module shouldn't be imported")
