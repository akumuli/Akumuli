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


def test_join_query_forward(columns, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    timedelta = end - begin

    query_params = {
        "output": { "format":  "csv" },
    }
    query = att.make_join_query(columns, begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = 0
    iterations = 0
    expected_tags = [
        "tag2=B",
        "tag2=C",
        "tag2=D",
    ]
    print("Test #1 - read forward, order by time")
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
    print("Test #1 - passed")


def test_join_query_backward(columns, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart + delta*(N - 1)
    end = dtstart - delta
    timedelta = begin - end

    query_params = {
        "output": { "format":  "csv" },
    }
    query = att.make_join_query(columns, begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N - 1
    iterations = 0
    expected_tags = [
        "tag2=B",
        "tag2=C",
        "tag2=D",
    ]
    print("Test #2 - read forward, order by time")
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            values = [float(it.strip()) for it in columns[2:]]
            exp_tags = expected_tags[(N - iterations - 1) % len(expected_tags)]

            for value in values:
                att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= delta
            exp_value -= 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(points_required, iterations))
    print("Test #2 - passed")

def count_elements(metric, tag, val, begin, end):
    query_params = {
        "output": { "format":  "csv" },
        "where": {
            tag: [val]
            }
        }
    query = att.make_aggregate_query(metric, begin, end, "count", **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = list(urlopen(queryurl, json.dumps(query)))
    for line in response:
        arr = line.split(',')
        return int(arr[-1])
    raise ValueError("Empty response")

def test_join_query_forward_by_series(columns, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    timedelta = end - begin

    query_params = {
        "output": { "format":  "csv" },
        "order-by": "series"
    }
    query = att.make_join_query(columns, begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = 0
    iterations = 0
    expected_tags = [
        "tag2=B",
        "tag2=C",
        "tag2=D",
    ]
    bsize = count_elements("col1", "tag2", "B", begin, end)
    csize = count_elements("col1", "tag2", "C", begin, end)
    dsize = count_elements("col1", "tag2", "D", begin, end)
    series_sizes = [
            bsize,
            bsize + csize,
            bsize + csize + dsize,
    ]
    nseries = len(expected_tags)
    print("Test #3 - read forward, order by series")
    prev_tag = None
    reset_ix = 0
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            values = [float(it.strip()) for it in columns[2:]]
            tagix = 0
            while iterations >= series_sizes[tagix]:
                tagix += 1

            exp_tags = expected_tags[tagix]

            if prev_tag != tagline:
                exp_ts = begin + delta*reset_ix
                exp_value = reset_ix
                prev_tag = tagline
                reset_ix += 1

            for value in values:
                att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts += nseries*delta
            exp_value += nseries
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #3 - passed")



def test_join_query_backward_by_series(columns, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart + delta*(N - 1)
    end = dtstart - delta
    timedelta = begin - end

    query_params = {
        "output": { "format":  "csv" },
        "order-by": "series"
    }
    query = att.make_join_query(columns, begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
    expected_tags = [
        "tag2=B",
        "tag2=C",
        "tag2=D",
    ]
    bsize = count_elements("col1", "tag2", "B", begin, end)
    csize = count_elements("col1", "tag2", "C", begin, end)
    dsize = count_elements("col1", "tag2", "D", begin, end)
    sizes = [
            bsize,
            csize,
            dsize,
            ]
    steps = [
            bsize,
            bsize + csize,
            bsize + csize + dsize,
            ]
    nseries = len(expected_tags)
    print("Test #4 - read forward, order by series")
    prev_tag = None
    reset_ix = 0
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            values = [float(it.strip()) for it in columns[2:]]
            tagix = 0
            while iterations >= steps[tagix]:
                tagix += 1

            exp_tags = expected_tags[tagix]

            if prev_tag != tagline:
                exp_ts = dtstart + reset_ix*delta + delta*(sizes[tagix]-1)*nseries
                exp_value = reset_ix + (sizes[tagix]-1)*nseries
                prev_tag = tagline
                reset_ix += 1

            for value in values:
                att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= nseries*delta
            exp_value -= nseries
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(points_required, iterations))
    print("Test #4 - passed")

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
        test_join_query_forward(columns, dt, delta, nmsgs)
        test_join_query_backward(columns, dt, delta, nmsgs)
        test_join_query_forward_by_series(columns, dt, delta, nmsgs)
        test_join_query_backward_by_series(columns, dt, delta, nmsgs)
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
