from __future__ import print_function
import os
import sys
import socket
import datetime
import time
import akumulid_test_tools as att
from akumulid_test_tools import retry, on_exit, api_test
import json
try:
    from urllib2 import urlopen, URLError
except ImportError:
    from urllib import urlopen, URLError
import traceback
import itertools
import math

HOST = '127.0.0.1'
TCPPORT = 8282
HTTPPORT = 8181

N_MSG = 1000

SERIES_TAGS = {
    "arch": ['x86', 'POWER'],
    "team": ['Stretch', 'Clench'],
    "azone": ['us-east-west', 'us-north-east'],
    "host": ['192.168.10.{0}'.format(i) for i in range(0, 255)],
}

_TAG_NAMES = [dict([(key, val[i % len(val)]) for key, val in SERIES_TAGS.iteritems()]) for i in xrange(0, N_MSG)]
ALL_TAG_COMBINATIONS = sorted(['arch={arch} azone={azone} host={host} team={team}'.format(**kwargs) for kwargs in _TAG_NAMES])

METRICS = [
    'cpu.user',
    'cpu.system',
    'df.free',
    'net.tcp.in',
    'net.tcp.out',
]

ALL_SERIES_NAMES = sorted([metric + ' ' + tagline for tagline in ALL_TAG_COMBINATIONS for metric in METRICS ])

@api_test("suggest metric name")
@retry(Exception, tries=3)
def test_suggest_metric():
    """Test suggest query."""
    query = {
        "select": "metric-names",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/suggest".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    all_metrics = []
    for line in response:
        try:
            name = line.strip()
            all_metrics.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(all_metrics) != set(METRICS):
        raise ValueError("Query results mismatch")

@api_test("suggest metric name by prefix")
@retry(Exception, tries=3)
def test_suggest_metric_prefix():
    """Test suggest query."""
    query = {
        "select": "metric-names",
        "starts-with": "net",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/suggest".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected_names = [name for name in METRICS if name.startswith('net')]

    all_metrics = []
    for line in response:
        try:
            name = line.strip()
            all_metrics.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(all_metrics) != set(expected_names):
        raise ValueError("Query results mismatch")

@api_test("suggest tag name")
@retry(Exception, tries=3)
def test_suggest_tag():
    """Test suggest query."""
    query = {
        "select": "tag-names",
        "metric": "cpu.user",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/suggest".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected = [name for name in SERIES_TAGS.iterkeys()]
    actual = []
    for line in response:
        try:
            name = line.strip()
            actual.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(actual) != set(expected):
        raise ValueError("Query results mismatch")

@api_test("suggest tag name by prefix")
@retry(Exception, tries=3)
def test_suggest_tag_prefix():
    """Test suggest query."""
    query = {
        "select": "tag-names",
        "metric": "cpu.user",
        "starts-with": "a",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/suggest".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected = [name for name in SERIES_TAGS.iterkeys() if name.startswith('a')]
    actual = []
    for line in response:
        try:
            name = line.strip()
            actual.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(actual) != set(expected):
        raise ValueError("Query results mismatch")

@api_test("suggest value")
@retry(Exception, tries=3)
def test_suggest_value():
    """Test suggest query."""
    query = {
        "select": "tag-values",
        "metric": "cpu.user",
        "tag": "host",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/suggest".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected = [value for value in SERIES_TAGS['host']]
    actual = []
    for line in response:
        try:
            name = line.strip()
            actual.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(actual) != set(expected):
        raise ValueError("Query results mismatch")

@api_test("suggest value by prefix")
@retry(Exception, tries=3)
def test_suggest_value_prefix():
    """Test suggest query."""
    query = {
        "select": "tag-values",
        "metric": "cpu.user",
        "tag": "host",
        "starts-with": "192.168.10.1",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/suggest".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected = [value for value in SERIES_TAGS['host'] if value.startswith('192.168.10.1')]
    actual = []
    for line in response:
        try:
            name = line.strip()
            actual.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(actual) != set(expected):
        raise ValueError("Query results mismatch")

@api_test("search all names")
@retry(Exception, tries=3)
def test_search_all_names():
    """Test search query."""
    query = {
        "select": "",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/search".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    all_names = []
    for line in response:
        try:
            name = line.strip()
            all_names.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(all_names) != set(ALL_SERIES_NAMES):
        raise ValueError("Query results mismatch")

@api_test("search names with metric")
@retry(Exception, tries=3)
def test_search_names_with_metric():
    """Test search query."""
    query = {
        "select": "df.free",
        "output": { "format":  "csv" },
    }
    queryurl = "http://{0}:{1}/api/search".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected = [name for name in ALL_SERIES_NAMES if name.startswith("df.free")]
    all_names = []
    for line in response:
        try:
            name = line.strip()
            all_names.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(all_names) != set(expected):
        raise ValueError("Query results mismatch")

@api_test("search names by tag")
@retry(Exception, tries=3)
def test_search_names_with_tag():
    """Test search query."""
    query = {
        "select": "df.free",
        "output": { "format":  "csv" },
        "where": { "team": "Stretch" },
    }
    queryurl = "http://{0}:{1}/api/search".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

    expected = [name for name in ALL_SERIES_NAMES if name.startswith("df.free") and name.count("team=Stretch")]
    all_names = []
    for line in response:
        try:
            name = line.strip()
            all_names.append(name)
        except:
            print("Error at line: {0}".format(line))
            raise
    if set(all_names) != set(expected):
        raise ValueError("Query results mismatch")

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
        nmsgs = N_MSG
        for metric in METRICS:
            print("Sending {0} messages through TCP...".format(nmsgs))
            for it in att.generate_messages(dt, delta, nmsgs, metric, **SERIES_TAGS):
                chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed

        # Test cases
        test_suggest_metric()
        test_suggest_metric_prefix()
        test_suggest_tag()
        test_suggest_tag_prefix()
        test_suggest_value()
        test_suggest_value_prefix()
        test_search_all_names()
        test_search_names_with_metric()
        test_search_names_with_tag()

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
