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


@api_test("read all data in backward direction")
def test_read_all_in_backward_direction(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*(N-1)
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
    exp_ts = begin
    exp_value = N-1
    iterations = 0
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


@api_test("group by tag in backward direction")
def test_group_by_tag_in_backward_direction(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    query_params = {
        "output": { "format":  "csv" },
        "group-by": [ "tag3" ],
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
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


@api_test("filter by tag")
def test_where_clause_in_backward_direction(dtstart, delta, N):
    """Filter data by tag"""
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    query_params = {
        "output": { "format":  "csv" },
        "where": {
            "tag2": ["C"], # read only odd
        }
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
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


@api_test("where + group-by")
def test_where_clause_with_groupby_in_backward_direction(dtstart, delta, N):
    """Filter data by tag and group by another tag"""
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    query_params = {
        "output": { "format":  "csv" },
        "group-by": [ "tag3" ],
        "where": {
            "tag2": ["C"], # read only odd
        }
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = N-1
    iterations = 0
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

@api_test("metadata query")
def test_metadata_query(tags):
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
        "select": "meta:names",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    for line in response:
        actual_series.append(line.strip())
    actual_series.sort()
    if actual_series != expected_series:
        print("Expected series: {0}".format(expected_series))
        print("Actual series: {0}".format(actual_series))
        raise ValueError("Output didn't match")


@api_test("filter by tag")
def test_read_in_forward_direction(dtstart, delta, N):
    """Read data in forward direction"""
    begin = dtstart
    end = dtstart + delta*(N + 1)
    timedelta = end - begin

    query_params = {
        "output": { "format":  "csv" },
    }
    query = att.makequery("test", begin, end, **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    exp_ts = begin
    exp_value = 0
    iterations = 0
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
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(points_required, iterations))


@api_test("aggregate all data")
def test_aggregate_all(dtstart, delta, N):
    """Aggregate all data and check result"""
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    query = att.make_aggregate_query("test", begin, end, "sum", output=dict(format='csv'))
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    expected_tags = [
        "tag3=D",
        "tag3=E",
        "tag3=F",
        "tag3=G",
        "tag3=H",
    ]
    M = N/10
    expected_values = [
        5*M**2 - 5*M,
        5*M**2 - 4*M,
        5*M**2 - 3*M,
        5*M**2 - 2*M,
        5*M**2 - M,
        5*M**2,
        5*M**2 + M,
        5*M**2 + 2*M,
        5*M**2 + 3*M,
        5*M**2 + 4*M,
        5*M**2 + 5*M,
    ]
    iterations = 0
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tag = expected_tags[iterations % len(expected_tags)]
            exp_val = expected_values[iterations % len(expected_values)]
            if abs(value - exp_val) > 10E-5:
                msg = "Invalid value, expected: {0}, actual: {1}".format(exp_val, value)
                print(msg)
                raise ValueError(msg)
            if tagline.endswith(exp_tag) == False:
                msg = "Unexpected tag value: {0}, expected: {1}".format(tagline, exp_tag)
                raise ValueError(msg)
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != len(expected_tags)*2:
        raise ValueError("Results incomplete")

@api_test("aggregate all data with group-by")
def test_aggregate_all_group_by(dtstart, delta, N):
    """Aggregate all data and check result"""
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    query_params = {
        "output": { "format":  "csv" },
        "group-by": [ "tag1" ],
    }
    query = att.make_aggregate_query("test", begin, end, "sum", **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    # All values will be collapsed into one!
    expected_tags = [
        "tag1=A",
    ]
    expected_values = [
        0.5*(N**2 - N)
    ]
    iterations = 0
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            if abs(value - expected_values[0]) > 10E-5:
                msg = "Invalid value, expected: {0}, actual: {1}".format(expected_values[0], value)
                print(msg)
                raise ValueError(msg)
            if tagline.endswith(expected_tags[0]) == False:
                msg = "Unexpected tag value: {0}, expected: {1}".format(tagline, expected_tags[0])
                raise ValueError(msg)
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != 1:
        raise ValueError("Bad number of results")


@api_test("aggregate + where")
def test_aggregate_where(dtstart, delta, N):
    """Aggregate all data and check result"""
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    query_params = {
        "output": { "format": "csv" },
        "where": {
            "tag3": ["D", "F", "H"],
        }
    }
    query = att.make_aggregate_query("test", begin, end, "sum", **query_params)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    expected_tags = [
        "tag3=D",
        "tag3=F",
        "tag3=H",
    ]
    M = N/10
    expected_values = [
        5*M**2 - 5*M,
        5*M**2 - 3*M,
        5*M**2 - M,  
        5*M**2,      
        5*M**2 + 2*M,
        5*M**2 + 4*M,
    ]
    iterations = 0
    for line in response:
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            exp_tag = expected_tags[iterations % len(expected_tags)]
            exp_val = expected_values[iterations % len(expected_values)]
            if abs(value - exp_val) > 10E-5:
                msg = "Invalid value, expected: {0}, actual: {1}".format(exp_val, value)
                print(msg)
                raise ValueError(msg)
            if tagline.endswith(exp_tag) == False:
                msg = "Unexpected tag value: {0}, expected: {1}".format(tagline, exp_tag)
                raise ValueError(msg)
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != len(expected_tags)*2:
        raise ValueError("Results incomplete")


@api_test("group aggregate all data")
def test_group_aggregate_all_forward(dtstart, delta, N, nsteps):
    """Aggregate all data and check result"""
    nseries = 10
    begin = dtstart
    end = dtstart + delta*(N + 1)
    step = int((delta * N * 1000).total_seconds() / nsteps)
    agg_funcs = ["min", "max", "count", "sum"]
    query = att.make_group_aggregate_query("test", begin, end, 
                                           agg_funcs, 
                                           "{0}ms".format(step), 
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
            min_value = float(columns[2].strip())
            max_value = float(columns[3].strip())
            cnt_value = float(columns[4].strip())
            sum_value = float(columns[4].strip())
            max_index = len(expected_tags) - 1
            exp_tag = expected_tags[iterations % len(expected_tags)]

            if tagline.endswith(exp_tag) == False:
                msg = "Unexpected tag value: {0}, expected: {1}".format(tagline, exp_tag)
                raise ValueError(msg)

            cnt_expected = N/nsteps/nseries
            if cnt_expected == 0:
                # expected count is less then 1 but not 0
                # there is more than 1 step per value in raw series
                cnt_expected = 1

            if cnt_value != cnt_expected:
                msg = "Invalid cnt value, expected: {0}, actual: {1}".format(cnt_expected, cnt_value)
                raise ValueError(msg)


            prev_val = registerd_values.get(tagline)
            if prev_val is not None:
                if abs(prev_val['max'] - min_value) - nseries > 10E-5:
                    msg = "Invalid value, expected: {0}, actual: {1}".format(prev_val['max'], min_value)
                    raise ValueError(msg)

            new_val = dict(max=max_value, min=min_value, cnt=cnt_value, sum=sum_value)
            registerd_values[tagline] = new_val

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations == 0:
        raise ValueError("Results incomplete")


@api_test("group aggregate all data")
def test_group_aggregate_all_backward(dtstart, delta, N, nsteps):
    """Aggregate all data and check result"""
    nseries = 10
    begin = dtstart + delta*(N-1)
    end = dtstart - delta
    step = int((delta * N * 1000).total_seconds() / nsteps)
    agg_funcs = ["min", "max", "count", "sum"]
    query = att.make_group_aggregate_query("test", begin, end, 
                                           agg_funcs, 
                                           "{0}ms".format(step), 
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
            min_value = float(columns[2].strip())
            max_value = float(columns[3].strip())
            cnt_value = float(columns[4].strip())
            sum_value = float(columns[4].strip())
            max_index = len(expected_tags) - 1
            exp_tag = expected_tags[max_index - (iterations % len(expected_tags))]

            if tagline.endswith(exp_tag) == False:
                msg = "Unexpected tag value: {0}, expected: {1}".format(tagline, exp_tag)
                raise ValueError(msg)

            cnt_expected = N/nsteps/nseries
            if cnt_expected == 0:
                # expected count is less then 1 but not 0
                # there is more than 1 step per value in raw series
                cnt_expected = 1

            if cnt_value != cnt_expected:
                msg = "Invalid cnt value, expected: {0}, actual: {1}".format(cnt_expected, cnt_value)
                raise ValueError(msg)


            prev_val = registerd_values.get(tagline)
            if prev_val is not None:
                if abs(prev_val['min'] - max_value) - nseries > 10E-5:
                    msg = "Invalid value, expected: {0}, actual: {1}".format(prev_val['min'], max_value)
                    raise ValueError(msg)

            new_val = dict(max=max_value, min=min_value, cnt=cnt_value, sum=sum_value)
            registerd_values[tagline] = new_val

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations == 0:
        raise ValueError("Results incomplete")


@api_test("PAA in backward direction")
def test_paa_in_backward_direction(dtstart, delta, N, fn, query):
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
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    exp_ts = begin
    iterations = 0
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


@api_test("late write")
def test_late_write(dtstart, delta, N, chan):
    """Read data in forward direction"""
    ts = dtstart
    message = att.msg(ts, 1.0, 'test', tag1='A', tag2='B', tag3='D')
    chan.send(message)
    resp = chan.recv().strip()
    if resp != '-DB late write':
        print(resp)
        raise ValueError("Late write not detected")


def check_error_message(dtstart, delta, N, query, errmsg):
    """Try to issue a broken query that doesn't match any existing time-series
    name in the storage."""
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    if type(query) is dict:
        query = json.dumps(query)
    response = urlopen(queryurl, query)
    lines = []
    for line in response:
        lines.append(line)
    if len(lines) != 1:
        raise ValueError("Error message expected")
    if not lines[0].startswith(errmsg):
        raise ValueError("Invalid error message")


@api_test("select query error message")
def select_from_nonexistent_metric(dtstart, delta, N):
    begin = dtstart
    end = dtstart + delta*(N + 1)
    query = att.make_select_query("err", begin, end)
    msg = "-not found"
    check_error_message(dtstart, delta, N, query, msg)

@api_test("aggregate query error message")
def aggregate_nonexistent_metric(dtstart, delta, N):
    begin = dtstart
    end = dtstart + delta*(N + 1)
    query = att.make_aggregate_query("err", begin, end, "sum")
    msg = "-not found"
    check_error_message(dtstart, delta, N, query, msg)

@api_test("group aggregate query error message")
def group_aggregate_nonexistent_metric(dtstart, delta, N):
    begin = dtstart
    end = dtstart + delta*(N + 1)
    query = att.make_group_aggregate_query("err", begin, end, ["sum"], "10ms")
    msg = "-not found"
    check_error_message(dtstart, delta, N, query, msg)

@api_test("join query error message")
def join_nonexistent_metrics(dtstart, delta, N):
    begin = dtstart
    end = dtstart + delta*(N + 1)
    query = att.make_join_query(["foo", "bar"], begin, end)
    msg = "-not found"
    check_error_message(dtstart, delta, N, query, msg)

def require_empty_response(query):
    """Make request and check that response is empty"""
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    lines = []
    for line in response:
        lines.append(line)
    if len(lines) != 0:
        print("Error: empty response expected, some data recieved. First 10 lines:")
        print("------------------------------")
        for line in lines[:10]:
            print(line.replace("\r", "\\r").replace("\n", "\\n"))
        print("------------------------------")
        raise ValueError("Empty response expected")

@api_test("select nonexistent time range")
def select_nonexistent_time_range(dtstart, delta, N):
    begin = dtstart + delta*(N*2)
    end = dtstart + delta*(N*3)
    query = att.make_select_query("test", begin, end)
    require_empty_response(query)

@api_test("aggregate nonexistent time range")
def aggregate_nonexistent_time_range(dtstart, delta, N):
    begin = dtstart + delta*(N*2)
    end = dtstart + delta*(N*3)
    query = att.make_aggregate_query("test", begin, end, "sum")
    require_empty_response(query)

@api_test("group-aggregate nonexistent time range")
def group_aggregate_nonexistent_time_range(dtstart, delta, N):
    begin = dtstart + delta*(N*2)
    end = dtstart + delta*(N*3)
    query = att.make_group_aggregate_query("test", begin, end, ["sum"], "10ms")
    require_empty_response(query)

@api_test("join nonexistent time range")
def join_nonexistent_time_range(dtstart, delta, N):
    begin = dtstart + delta*(N*2)
    end = dtstart + delta*(N*3)
    query = att.make_join_query(["test", "test"], begin, end)
    require_empty_response(query)

def check_bad_query_handling():
    """Send hideous queries to akumuli. Check proper error handling"""
    queries = {
        "totally empty query": "",

        "empty json doc": """
            {}
        """,
        "invalid keyword": """
            {
                "foo": "bar"
            }
        """,
        "invalid json": """
            {
                "select": "test",
                "range": { "from": "20170107T120300", "to": "20170107T120300" }
        """,
        "invalid timestamp": """
            {
                "select": "test",
                "range": { "from": "2017-01-07 12:03:00", "to": "20170107T120300" }
            }
        """,
        "timestamp out of range": """
            {
                "select": "test",
                "range": { "from": "20172107T120300", "to": "20170107T120300" }
            }
        """,
        "bad aggregation function": """
            {
                "group-aggregate": { "metric": "test", "func": "bad_func_name", "step": "1s" },
                "range": { "from": "20170107T120300", "to": "20170107T120300" }
            }
        """,
        "bad aggregation step": """
            {
                "group-aggregate": { "metric": "test", "sum": "bad_func_name", "step": "1 sec." },
                "range": { "from": "20170107T120300", "to": "20170107T120300" }
            }
        """,
        "bad join": """
            {
                "join": "test",
                "range": { "from": "20170107T120300", "to": "20170107T120300" }
            }
        """,
        "bad aggregate": """
            {
                "aggregate": "test",
                "range": { "from": "20170107T120300", "to": "20170107T120300" }
            }
        """
    }
    for title, query in queries.iteritems():
        @api_test(title)
	@retry(Exception, tries=3)
        def test():
            queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
            try:
                response = urlopen(queryurl, query)
                lines = []
                for line in response:
                    lines.append(line)
                if len(lines) > 1:
                    print("Error: error message expected, some data recieved. First 10 lines:")
                    print("------------------------------")
                    for line in lines[:10]:
                        print(line.replace("\r", "\\r").replace("\n", "\\n"))
                    print("------------------------------")
                    raise ValueError("Error expected")
                elif len(lines) == 0:
                    print("Error: error message expected, empty response received")
                    raise ValueError("Error expected")
                else:
                    if not lines[0].startswith("-query parsing error"):
                        raise ValueError("Invalid response")
            except HTTPError as e:
                raise ValueError("Invalid response: " + str(e))
        test()

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
            "tag2": ['B', 'C'],
            "tag3": ['D', 'E', 'F', 'G', 'H'],
        }
        for it in att.generate_messages(dt, delta, nmsgs, 'test', **tags):
            chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed


        # Test normal operation
        test_read_all_in_backward_direction(dt, delta, nmsgs)
        test_group_by_tag_in_backward_direction(dt, delta, nmsgs)
        test_where_clause_in_backward_direction(dt, delta, nmsgs)
        test_where_clause_with_groupby_in_backward_direction(dt, delta, nmsgs)
        test_metadata_query(tags)
        test_read_in_forward_direction(dt, delta, nmsgs)
        test_late_write(dt, delta, nmsgs, chan)
        test_aggregate_all(dt, delta, nmsgs)
        test_aggregate_all_group_by(dt, delta, nmsgs)
        test_aggregate_where(dt, delta, nmsgs)
        test_group_aggregate_all_forward (dt, delta, nmsgs, 10)
        test_group_aggregate_all_forward (dt, delta, nmsgs, 100)
        test_group_aggregate_all_forward (dt, delta, nmsgs, 1000)
        test_group_aggregate_all_forward (dt, delta, nmsgs, 100000)
        test_group_aggregate_all_backward(dt, delta, nmsgs, 10)
        test_group_aggregate_all_backward(dt, delta, nmsgs, 100)
        test_group_aggregate_all_backward(dt, delta, nmsgs, 1000)
        test_group_aggregate_all_backward(dt, delta, nmsgs, 100000)
        # Test error handling
        select_from_nonexistent_metric(dt, delta, nmsgs)
        aggregate_nonexistent_metric(dt, delta, nmsgs)
        group_aggregate_nonexistent_metric(dt, delta, nmsgs)
        join_nonexistent_metrics(dt, delta, nmsgs)
        select_nonexistent_time_range(dt, delta, nmsgs)
        aggregate_nonexistent_time_range(dt, delta, nmsgs)
        group_aggregate_nonexistent_time_range(dt, delta, nmsgs)
        join_nonexistent_time_range(dt, delta, nmsgs)
        check_bad_query_handling()
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)
    on_exit()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
