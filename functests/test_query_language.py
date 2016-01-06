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
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N-iterations-1) % len(expected_tags)]

            att.check_values(exp_tags, tagline, 'EQ', exp_ts, timestamp, exp_value*1.0, value, iterations)

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
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N - iterations - 1) % len(expected_tags)]

            att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= 2*delta
            exp_value -= 2
            iterations += 2
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #3 passed")


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
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(N - iterations - 1) % len(expected_tags)]

            att.check_values(exp_tags, tagline, 'EQ', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts -= 2*delta
            exp_value -= 2
            iterations += 2
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #4 passed")

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
    print("Test #5 passed")


def test_read_in_forward_direction(dtstart, delta, N):
    """Read data in forward direction"""
    window = att.get_window_width()
    end = dtstart + delta*(N-1) - window
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
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tags = expected_tags[(iterations) % len(expected_tags)]

            att.check_values(exp_tags, tagline, 'ENDS', exp_ts, timestamp, exp_value*1.0, value, iterations)

            exp_ts += delta
            exp_value += 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != points_required:
        raise ValueError("Expect {0} data points, get {1} data points".format(points_required, iterations))
    print("Test #6 passed")


def test_paa_in_backward_direction(testname, dtstart, delta, N, fn, query):
    expected_values = [
        reversed(range(9, 100000, 10)),
        reversed(range(8, 100000, 10)),
        reversed(range(7, 100000, 10)),
        reversed(range(6, 100000, 10)),
        reversed(range(5, 100000, 10)),
        reversed(range(4, 100000, 10)),
        reversed(range(3, 100000, 10)),
        reversed(range(2, 100000, 10)),
        reversed(range(1, 100000, 10)),
        reversed(range(0, 100000, 10)),
    ]

    def sliding_window(values, winlen, func):
        top = [0]*winlen
        for ix, it in enumerate(values):
            k = ix % winlen
            top[k] = it
            if (ix + 1) % winlen == 0:
                yield func(top)

    def round_robin(sequences, maxlen):
        l = len(sequences)
        for i in xrange(0, maxlen):
            seq = sequences[i % l]
            it = seq.next()
            yield it

    begin = dtstart + delta*N
    end = dtstart
    query_params = {
        "sample": [{   "name": query }],
        "output":  { "format": "csv" },
        "group-by":{   "time": "1s"  },
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    exp_ts = begin
    iterations = 0
    print("{0} - PAA".format(testname))
    expected_tags = [
        "tag3=H",
        "tag3=G",
        "tag3=F",
        "tag3=E",
        "tag3=D",
    ]
    sequences = [sliding_window(it, 100, fn) for it in expected_values]
    exp_values = round_robin(sequences, N)
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())

            exp_tags = expected_tags[iterations % len(expected_tags)]
            exp_value = exp_values.next()
            if timestamp != exp_ts:
                raise ValueError("Expected {0}, actual {1}".format(exp_ts, timestamp))
            if value != exp_value:
                raise ValueError("Expected {0}, actual {1}".format(exp_value, value))
            if not tagline.endswith(exp_tags):
                raise ValueError("Expected {0}, actual {1}".format(exp_tags, tagline))

            if (iterations + 1) % 10 == 0:
                exp_ts -= datetime.timedelta(seconds=1)
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != 990:
        raise ValueError("Expect {0} data points, get {1} data points".format(990, iterations))
    print("{0} passed".format(testname))


def test_late_write(dtstart, delta, N, chan):
    """Read data in forward direction"""
    print("Test #7 - late write")
    window = att.get_window_width()
    ts = dtstart + delta*(N-1) - 2*window
    message = att.msg(ts, 1.0, 'test', key='value')
    chan.send(message)
    resp = chan.recv().strip()
    if resp != '-DB late write':
        print(resp)
        raise ValueError("Late write not detected")
    print("Test #7 passed")

def med(buf):
    buf = sorted(buf)
    return buf[len(buf)/2]


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

        chan = att.TCPChan(HOST, TCPPORT)

        # fill data in
        dt = datetime.datetime.utcnow().replace(second=0, microsecond=0)
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
        test_late_write(dt, delta, nmsgs, chan)
        test_paa_in_backward_direction("Test #8", dt, delta, nmsgs, lambda buf: float(sum(buf))/len(buf), "paa")
        test_paa_in_backward_direction("Test #9", dt, delta, nmsgs, med, "median-paa")
        test_paa_in_backward_direction("Test #A", dt, delta, nmsgs, max, "max-paa")
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
