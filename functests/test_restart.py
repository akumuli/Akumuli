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
import itertools
import math

HOST = '127.0.0.1'
TCPPORT = 8282
HTTPPORT = 8181

def test_read_all(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*N
    end = dtstart - delta
    query = att.makequery("test", begin, end, output=dict(format='csv'))
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    exp_ts = dtstart + delta*(N-1)
    exp_value = N-1
    iterations = 0
    print("Test - read all data in backward direction")
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N-iterations-1) % len(expected_tags)]

            att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= delta
            exp_value -= 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test passed")


def main(path):
    akumulid = att.create_akumulid(path)
    # Reset database
    akumulid.delete_database()
    akumulid.create_database()

    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)

    nmsgs = 100000
    dt = datetime.datetime.utcnow() - (datetime.timedelta(milliseconds=1)*nmsgs)
    delta = datetime.timedelta(milliseconds=1)
    try:
        chan = att.TCPChan(HOST, TCPPORT)

        # fill data in
        print("Sending {0} messages through TCP...".format(nmsgs))
        tags = {
            "tag1": ['A'],
            "tag2": ['B', 'C'],
            "tag3": ['D', 'E', 'F', 'G', 'H'],
        }
        for it in att.generate_messages(dt, delta, nmsgs, 'test', **tags):
            chan.send(it)

        time.sleep(5)  # wait untill all messagess will be processed
        print("Trying to close channel")
        chan.close()

        test_read_all(dt, delta, nmsgs)
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)
        print("Server stopped")

    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    print("Server started")
    try:
        test_read_all(dt, delta, nmsgs)

        # Try to write new data
        dt = datetime.datetime.utcnow()
        chan = att.TCPChan(HOST, TCPPORT)

        # fill data in
        print("Sending {0} messages through TCP second time...".format(nmsgs))
        tags = {
            "tag1": ['A'],
            "tag2": ['B', 'C'],
            "tag3": ['D', 'E', 'F', 'G', 'H'],
        }
        for it in att.generate_messages(dt, delta, nmsgs, 'test', **tags):
            chan.send(it)
        time.sleep(5)
        test_read_all(dt, delta, nmsgs)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)

    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
