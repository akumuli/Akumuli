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


@api_test("group aggregate join forward")
def test_group_aggregate_join_forward(dtstart, delta, N, step, agg_func):
    """Aggregate all data and check result"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    metrics = [ "cpu.user", "cpu.syst" ]
    query = att.make_group_aggregate_join_query(metrics, agg_func, begin, end, 
                                                step,
                                                output={"format": "csv"},
                                                where={"tag3": "D", "tag2": "C"},
                                                apply=[
                                                    { "name": "eval2", "expr": "cpu.user - cpu.syst" }
                                                ])

    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    iterations = 0
    exptimestamp = begin
    for line in response:
        try:
            columns = line.split(',')
            if len(columns) != 3:
                raise ValueError("Unexpected number of columns in the output")
            sname = columns[0]
            if not sname.startswith("|".join(metrics)):
                raise ValueError("Unexpected series name {0}".format(columns[0]))

            timestamp = att.parse_timestamp(columns[1].strip())
            if timestamp != exptimestamp:
                tserrormsg = "Actual timestamp value: {0}\nExpected timestamp value {1}".format(columns[1].strip(), exptimestamp)
                raise ValueError(tserrormsg)
            exptimestamp += delta

            # Check that all three values are the same
            zero = int(columns[2])
            if zero != 0:
                raise ValueError("Unexpected value {0}".format(zero))

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != N:
        raise ValueError("Invalid number of result {0} expected {1} received".format(N, iterations))

@api_test("group aggregate join backward")
def test_group_aggregate_join_backward(dtstart, delta, N, step, agg_func):
    """Aggregate all data and check result"""
    begin = dtstart + delta * N
    end = dtstart - delta
    metrics = [ "cpu.user", "cpu.syst" ]
    query = att.make_group_aggregate_join_query(metrics, agg_func, begin, end, 
                                                step,
                                                output={"format": "csv"},
                                                where={"tag3": "D", "tag2": "C"},
                                                apply=[
                                                    { "name": "eval2", "expr": "cpu.user - cpu.syst" }
                                                ])

    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    iterations = 0
    exptimestamp = begin
    for line in response:
        try:
            columns = line.split(',')
            if len(columns) != 3:
                raise ValueError("Unexpected number of columns in the output")
            sname = columns[0]
            if not sname.startswith("|".join(metrics)):
                raise ValueError("Unexpected series name {0}".format(columns[0]))

            timestamp = att.parse_timestamp(columns[1].strip())
            if timestamp != exptimestamp:
                tserrormsg = "Actual timestamp value: {0}\nExpected timestamp value {1}".format(columns[1].strip(), exptimestamp)
                raise ValueError(tserrormsg)
            exptimestamp -= delta

            # Check that all three values are the same
            zero = int(columns[2])
            if zero != 0:
                raise ValueError("Unexpected value {0}".format(zero))

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != N:
        raise ValueError("Invalid number of result {0} expected {1} received".format(N, iterations))


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
        for it in att.generate_messages(dt, delta, nmsgs, 'cpu.user', **tags):
            chan.send(it)
        for it in att.generate_messages(dt, delta, nmsgs, 'cpu.syst', **tags):
            chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed


        # Run tests
        test_group_aggregate_join_forward(dt, datetime.timedelta(minutes=1),  1440, '1m',  'min')
        test_group_aggregate_join_forward(dt, datetime.timedelta(minutes=10), 144,  '10m', 'min')
        test_group_aggregate_join_forward(dt, datetime.timedelta(minutes=60), 24,   '1h',  'min')
        test_group_aggregate_join_forward(dt, datetime.timedelta(minutes=1),  1440, '1m',  'max')
        test_group_aggregate_join_forward(dt, datetime.timedelta(minutes=10), 144,  '10m', 'max')
        test_group_aggregate_join_forward(dt, datetime.timedelta(minutes=60), 24,   '1h',  'max')

        test_group_aggregate_join_backward(dt, datetime.timedelta(minutes=1),  1440, '1m',  'min')
        test_group_aggregate_join_backward(dt, datetime.timedelta(minutes=10), 144,  '10m', 'min')
        test_group_aggregate_join_backward(dt, datetime.timedelta(minutes=60), 24,   '1h',  'min')
        test_group_aggregate_join_backward(dt, datetime.timedelta(minutes=1),  1440, '1m',  'max')
        test_group_aggregate_join_backward(dt, datetime.timedelta(minutes=10), 144,  '10m', 'max')
        test_group_aggregate_join_backward(dt, datetime.timedelta(minutes=60), 24,   '1h',  'max')
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
