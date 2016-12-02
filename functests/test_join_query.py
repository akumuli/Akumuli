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


def test_join_query(columns, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    timedelta = end - begin

    query_params = {
        "output": { "format":  "csv" },
    }
    query = att.make_join_query(columns, begin, end, **query_params)
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = 0
    iterations = 0
    expected_tags = [
        "tag2=B",
        "tag2=C",
        "tag2=D",
    ]
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            values = [float(it.strip()) for it in columns[2:]]
            exp_tags = expected_tags[(iterations) % len(expected_tags)]

            for value in values:
                att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts += delta
            exp_value += 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(points_required, iterations))
    print("Test #6 passed")


def med(buf):
    buf = sorted(buf)
    return buf[len(buf)/2]


def main(path):
    akumulid = att.create_akumulid(path)

    # Reset database
    akumulid.delete_database()
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    try:

        chan = att.TCPChan(HOST, TCPPORT)

        # fill data in
        dt = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 100000
        print("Sending {0} messages through TCP...".format(nmsgs))
        tags = {
            "tag1": ['A'],
            "tag2": ['B', 'C', 'D'],
        }

        for it in att.generate_messages(dt, delta, nmsgs, 'col1', **tags):
            chan.send(it)
        for it in att.generate_messages(dt, delta, nmsgs, 'col2', **tags):
            chan.send(it)

        time.sleep(5)  # wait untill all messagess will be processed
        
        columns = [ 'col1', 'col2' ]
        test_join_query(columns, dt, delta, nmsgs)
    except:
        traceback.print_exc()
        sys.exit(1)
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
