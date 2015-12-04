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
import traceback

HOST = '127.0.0.1'
TCPPORT = 8282
HTTPPORT = 8181

def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    return datetime.datetime.strptime(ts.rstrip('0'), "%Y%m%dT%H%M%S.%f")

class TCPChan:
    def __init__(self, HOST, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((HOST, port))

    def send(self, data):
        self.__sock.send(data)


def test_read_all_in_backward_direction(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*N
    end = dtstart
    query = att.makequery(begin, end, output=dict(format='csv'))
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N
    iterations = 0
    for line in response:
        columns = line.split(',')
        tagline = columns[0].strip()
        timestamp = parse_timestamp(columns[1].strip())
        value = float(columns[2].strip())
        # Check values
        if timestamp != exp_ts:
            raise ValueError("Invalid timestamp, expected: {0}, actual: {1}".format(exp_ts, timestamp))
        if value != 1.0*exp_value:
            raise ValueError("Invalid value, expected: {0}, actual: {1}".format(exp_value, value))
        exp_ts -= delta
        exp_value -= 1
        iterations += 1

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #1 passed")


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

        chan = TCPChan(HOST, TCPPORT)

        # fill data in
        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 100000
        print("Sending {0} messages through TCP...".format(nmsgs))
        for it in att.generate_messages(dt, delta, nmsgs, 'temp', tag='test'):
            chan.send(it)

        test_read_all_in_backward_direction(dt, delta, nmsgs)
    except:
        traceback.print_exc()
        sys.exit(1)
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
