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

def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    return datetime.datetime.strptime(ts.rstrip('0'), "%Y%m%dT%H%M%S.%f")

class TCPChan:
    def __init__(self, HOST, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((HOST, port))

    def send(self, data):
        self.__sock.send(data)

def check_values(exp_tags, act_tags, tags_cmp_method, exp_ts, act_ts, exp_value, act_value, iterations):
    if tags_cmp_method == 'EQ':
        if act_tags != exp_tags:
            errormsg = "Invalid tags, expected: {0}, actual: {1}, iter: {2}".format(exp_tags, act_tags, iterations)
            raise ValueError(errormsg)
    elif tags_cmp_method == 'ENDS':
        if not act_tags.endswith(exp_tags):
            errormsg = "Invalid tags, expected suffix: {0}, actual: {1}, iter: {2}".format(exp_tags, act_tags, iterations)
            raise ValueError(errormsg)
    if act_ts != exp_ts:
        errormsg = "Invalid timestamp, expected: {0}, actual: {1}, iter: {2}".format(exp_ts, act_ts, iterations)
        raise ValueError(errormsg)
    if act_value != exp_value:
        errormsg = "Invalid value, expected: {0}, actual: {1}, iter: {2}".format(exp_value, act_value, iterations)
        raise ValueError(errormsg)

def test_read_all_in_backward_direction(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*(N-1)
    end = dtstart
    query = att.makequery("test", begin, end, output=dict(format='csv'))
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    exp_ts = begin
    exp_value = N-1
    iterations = 0
    print("Test #1 - read all data in backward direction")
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N-iterations-1) % len(expected_tags)]

            check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= delta
            exp_value -= 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #1 passed")


def test_group_by_tag_in_backward_direction(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*(N-1)
    end = dtstart
    query_params = {
        "output": { "format":  "csv" },
        "group-by": {  "tag": "tag3" },
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
    print("Test #2 - group by tag in backward direction")
    expected_tags = [
        "test tag3=D",
        "test tag3=E",
        "test tag3=F",
        "test tag3=G",
        "test tag3=H",
    ]
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N-iterations-1) % len(expected_tags)]

            check_values(exp_tags, tagline, 'EQ', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= delta
            exp_value -= 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #2 passed")

def test_where_clause_in_backward_direction(dtstart, delta, N):
    """Filter data by tag"""
    begin = dtstart + delta*(N-1)
    end = dtstart
    query_params = {
        "output": { "format":  "csv" },
        "where": {
            "tag2": ["C"], # read only odd
        }
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
    print("Test #3 - filter by tag")
    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N - iterations - 1) % len(expected_tags)]

            check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= 2*delta
            exp_value -= 2
            iterations += 2
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("test #3 passed")


def test_where_clause_with_groupby_in_backward_direction(dtstart, delta, N):
    """Filter data by tag and group by another tag"""
    begin = dtstart + delta*(N-1)
    end = dtstart
    query_params = {
        "output": { "format":  "csv" },
        "group-by": { "tag": "tag3" },
        "where": {
            "tag2": ["C"], # read only odd
        }
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
    print("Test #4 - where + group-by")
    expected_tags = [
        "test tag3=D",
        "test tag3=E",
        "test tag3=F",
        "test tag3=G",
        "test tag3=H",
    ]
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N - iterations - 1) % len(expected_tags)]

            check_values(exp_tags, tagline, 'EQ', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= 2*delta
            exp_value -= 2
            iterations += 2
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("test #4 passed")

def test_metadata_query(tags):
    print("Test #5 - metadata query")
    # generate all possible series
    taglist = sorted(itertools.product(*tags.values()))
    expected_series = []
    for it in taglist:
        kv = sorted(zip(tags.keys(), it))
        series = "test " + " ".join(["{0}={1}".format(tag, value) for tag, value in kv])
        expected_series.append(series)
    expected_series.sort()

    # read metadata from server
    actual_series = []
    query = {
        "select": "names",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    for line in response:
        actual_series.append(line.strip())
    actual_series.sort()
    if actual_series != expected_series:
        print("Expected series: {0}".format(expected_series))
        print("Actual series: {0}".format(actual_series))
        raise ValueError("Output didn't match")
    print("test #5 passed")


def test_read_in_forward_direction(dtstart, delta, N):
    """Read data in forward direction"""
    window = att.get_window_width()
    end = dtstart + delta*N - 2*window
    begin = dtstart
    timedelta = end - begin
    points_required = int(math.ceil((timedelta.seconds*1000000.0 + timedelta.microseconds) / (delta.seconds*1000000.0 + delta.microseconds))) + 1
    # We need to add 1 because query will include both begin and end timestamps.

    query_params = {
        "output": { "format":  "csv" },
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = 0
    iterations = 0
    print("Test #6 - filter by tag")
    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(iterations) % len(expected_tags)]

            check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts += delta
            exp_value += 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != points_required:
        raise ValueError("Expect {0} data points, get {1} data points".format(points_required, iterations))
    print("test #6 passed")


def main(path, debug=False):
    if not os.path.exists(path):
        print("Path {0} doesn't exists".format(path))
        sys.exit(1)

    akumulid = att.Akumulid(path)
    if not debug:
        # Reset database
        akumulid.delete_database()
        akumulid.create_database()
        # start ./akumulid server
        print("Starting server...")
        akumulid.serve()
        time.sleep(5)
    else:
        print("Akumulid should be started first")
    try:

        chan = TCPChan(HOST, TCPPORT)

        # fill data in
        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 100000
        print("Sending {0} messages through TCP...".format(nmsgs))
        tags = {
            "tag1": ['A'],
            "tag2": ['B', 'C'],
            "tag3": ['D', 'E', 'F', 'G', 'H'],
        }
        for it in att.generate_messages(dt, delta, nmsgs, 'test', **tags):
            chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed

        test_read_all_in_backward_direction(dt, delta, nmsgs)
        test_group_by_tag_in_backward_direction(dt, delta, nmsgs)
        test_where_clause_in_backward_direction(dt, delta, nmsgs)
        test_where_clause_with_groupby_in_backward_direction(dt, delta, nmsgs)
        test_metadata_query(tags)
        test_read_in_forward_direction(dt, delta, nmsgs)
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        if not debug:
            print("Stopping server...")
            akumulid.stop()
            time.sleep(5)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2] == 'debug' if len(sys.argv) == 3 else False)
else:
    raise ImportError("This module shouldn't be imported")
