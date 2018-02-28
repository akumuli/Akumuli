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

def make_query(metric, from_, to, lowerbound, upperbound, **query_params):
    query_params["filter"] = {
        "lt": upperbound,
        "gt": lowerbound,
    }
    query = att.make_select_query(metric, from_, to, **query_params)
    return query

def run_query(column, begin, end, thresholds, N, **query_params):
    query_params["output"] = { "format":  "csv" }

    query = make_query(column, 
                       begin, 
                       end, 
                       thresholds[0], 
                       thresholds[1], 
                       **query_params)

    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)

    body = json.dumps(query)
    response = urlopen(queryurl, json.dumps(query))

    exp_values = range(thresholds[0], thresholds[1])
    iterations = 0
    exp_tags = [
        column + " tag1=A tag2=B",
        column + " tag1=A tag2=C",
        column + " tag1=A tag2=D",
    ]

    for line in response:
        try:
            columns = line.split(',')
            sname = columns[0].strip()
            value = float(columns[2].strip())

            if not sname in exp_tags:
                raise ValueError("Unexpected tags")

            if not value in exp_values:
                raise ValueError("Unexpected value")

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations == 0:
        raise ValueError("No data received")

att.api_test("Test filter query forward")
def test_filter_query_forward(column, dtstart, delta, thresholds, N):
    """Read data in forward direction"""

    begin = dtstart
    end = dtstart + delta*(N + 1)

    run_query(column, begin, end, thresholds, N)


att.api_test("Test filter query backward")
def test_filter_query_backward(column, dtstart, delta, thresholds, N):
    """Read data in backward direction"""

    end = dtstart
    begin = dtstart + delta*(N + 1)

    run_query(column, begin, end, thresholds, N)

att.api_test("Test filter query forward, order by time")
def test_filter_query_forward_by_time(column, dtstart, delta, thresholds, N):
    """Read data in forward direction, order by time"""

    begin = dtstart
    end = dtstart + delta*(N + 1)

    q = {
        "order-by": "time"
    }

    run_query(column, begin, end, thresholds, N, **q)

att.api_test("Test filter query backward, order by time")
def test_filter_query_backward_by_time(column, dtstart, delta, thresholds, N):
    """Read data in backward direction, order by time"""

    end = dtstart
    begin = dtstart + delta*(N + 1)

    q = {
        "order-by": "time"
    }

    run_query(column, begin, end, thresholds, N, **q)

att.api_test("Test filter query no results")
def test_filter_query_empty(column, dtstart, delta, N):
    """Read data in forward direction"""

    begin = dtstart
    end = dtstart + delta*(N + 1)

    query_params = {
        "output" : { "format":  "csv" }
    }

    query = make_query(column, 
                       begin, 
                       end, 
                       -2000, 
                       -1000, 
                       **query_params)

    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)

    body = json.dumps(query)
    response = urlopen(queryurl, json.dumps(query))

    for line in response:
        raise ValueError("Unexpected value " + line)

def run_join_query(columns, thresholds, begin, end, **query_params):
    flt = {}
    for ix, column in enumerate(columns):
        flt[column] = dict(gt=thresholds[ix][0],
                                     lt=thresholds[ix][1])

    query_params["output"] = { "format":  "csv" }
    query_params["filter"] = flt

    query = att.make_join_query(columns, begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    iterations = 0
    col1_range = range(thresholds[0][0], thresholds[0][1])
    col2_range = range(thresholds[1][0], thresholds[1][1])
    expected_tags = [
        "col1|col2 tag1=A tag2=B",
        "col1|col2 tag1=A tag2=C",
        "col1|col2 tag1=A tag2=D",
    ]
    
    for ix, line in enumerate(response):
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            values = [it.strip() for it in columns[2:]]

            if tagline not in expected_tags:
                raise ValueError("Unexpected series name")

            if values[0] != '' and float(values[0]) not in col1_range:
                raise ValueError("Unexpected col1 value")

            if values[1] != '' and float(values[1]) not in col2_range:
                raise ValueError("Unexpected col2 value")

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations == 0:
        raise ValueError("No data returned")

att.api_test("Test join query forward")
def test_join_query_forward(columns, thresholds, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    run_join_query(columns, thresholds, begin, end)

att.api_test("Test join query backward")
def test_join_query_backward(columns, thresholds, dtstart, delta, N):
    """Read data in backward direction"""
    end = dtstart
    begin = dtstart + delta*(N + 1)
    run_join_query(columns, thresholds, begin, end)

att.api_test("Test join query forward, order by time")
def test_join_query_forward_by_time(columns, thresholds, dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    q = { "order-by": "time" }
    run_join_query(columns, thresholds, begin, end, **q)

att.api_test("Test join query backward, order by time")
def test_join_query_backward_by_time(columns, thresholds, dtstart, delta, N):
    """Read data in backward direction"""
    end = dtstart
    begin = dtstart + delta*(N + 1)
    q = { "order-by": "time" }
    run_join_query(columns, thresholds, begin, end, **q)

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

        values = [float(x) for x in range(-100, 100)]

        for it in att.generate_messages4(dt, delta, nmsgs, 'col1', values, **tags):
            chan.send(it)
        for it in att.generate_messages4(dt, delta, nmsgs, 'col2', values, **tags):
            chan.send(it)

        time.sleep(5)  # wait untill all messagess will be processed
        
        test_filter_query_forward('col1', dt, delta, [-20, 20], nmsgs)
        test_filter_query_backward('col1', dt, delta, [-20, 20], nmsgs)
        test_filter_query_forward_by_time('col1', dt, delta, [-20, 20], nmsgs)
        test_filter_query_backward_by_time('col1', dt, delta, [-20, 20], nmsgs)
        test_filter_query_empty('col1', dt, delta, nmsgs)
        test_join_query_forward(['col1', 'col2'], 
                                [[-20, 20], [40, 60]],
                                dt, delta, nmsgs)
        test_join_query_backward(['col1', 'col2'], 
                                [[-20, 20], [40, 60]],
                                dt, delta, nmsgs)
        test_join_query_forward_by_time(['col1', 'col2'], 
                                        [[-20, 20], [40, 60]],
                                        dt, delta, nmsgs)
        test_join_query_backward_by_time(['col1', 'col2'], 
                                        [[-20, 20], [40, 60]],
                                        dt, delta, nmsgs)
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
