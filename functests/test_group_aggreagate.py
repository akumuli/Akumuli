from __future__ import print_function
import os
import sys
import socket
import datetime
import time
import akumulid_test_tools as att
from akumulid_test_tools import retry, api_test, on_exit
import json
try:
    from urllib2 import urlopen, HTTPError, URLError
except ImportError:
    from urllib import urlopen, HTTPError, URLError
import traceback
import itertools
import math

HOST = '127.0.0.1'
TCPPORT = 8282
HTTPPORT = 8181


@api_test("group aggregate all data")
def test_group_aggregate_all_forward(dtstart, delta, N, step):
    """Aggregate all data and check result"""
    nseries = 10
    begin = dtstart
    end = dtstart + delta*(N + 1)
    agg_funcs = ["min", "max", "count", "sum"]
    query = att.make_group_aggregate_query("test", begin, end, 
                                           agg_funcs, 
                                           step,
                                           output=dict(format='csv'))
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    registerd_values = {}
    iterations = 0
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())

            tserrormsg = "Unexpected timestamp value: {0}".format(columns[1].strip())
            if timestamp.second != dtstart.second:
                raise ValueError(tserrormsg)
            if timestamp.microsecond != dtstart.microsecond:
                raise ValueError(tserrormsg)

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations == 0:
        raise ValueError("Results incomplete")


@api_test("group aggregate all data")
def test_group_aggregate_all_backward(dtstart, delta, N, step):
    """Aggregate all data and check result"""
    nseries = 10
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    agg_funcs = ["min", "max", "count", "sum"]
    query = att.make_group_aggregate_query("test", begin, end, 
                                           agg_funcs, 
                                           step,
                                           output=dict(format='csv'))
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    registerd_values = {}
    iterations = 0
    for line in response:
        try:
            columns = line.split(',')
            timestamp = att.parse_timestamp(columns[1].strip())

            tserrormsg = "Unexpected timestamp value: {0}".format(columns[1].strip())
            if timestamp.second != dtstart.second:
                raise ValueError(tserrormsg)
            if timestamp.microsecond != dtstart.microsecond:
                raise ValueError(tserrormsg)

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations == 0:
        raise ValueError("Results incomplete")


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
        dt = datetime.datetime.utcnow().replace(hour=0, minute=0, second=10, microsecond=0)
        delta = datetime.timedelta(seconds=1)
        nmsgs = 3600*24
        print("Sending {0} messages through TCP...".format(nmsgs))
        tags = {
            "tag1": ['A'],
            "tag2": ['B', 'C'],
            "tag3": ['D', 'E', 'F', 'G', 'H'],
        }
        for it in att.generate_messages(dt, delta, nmsgs, 'test', **tags):
            chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed


        # Test normal operation
        test_group_aggregate_all_forward (dt, delta, nmsgs, '1m')
        test_group_aggregate_all_forward (dt, delta, nmsgs, '10m')
        test_group_aggregate_all_forward (dt, delta, nmsgs, '1h')
        test_group_aggregate_all_backward(dt, delta, nmsgs, '1m')
        test_group_aggregate_all_backward(dt, delta, nmsgs, '10m')
        test_group_aggregate_all_backward(dt, delta, nmsgs, '1h')
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)
    on_exit()

if __name__ == '__main__':
    print(' '.join(sys.argv))
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
