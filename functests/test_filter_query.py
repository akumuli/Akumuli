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

att.api_test("Test filter query forward")
def test_filter_query_forward(column, dtstart, delta, thresholds, N):
    """Read data in forward direction"""

    begin = dtstart
    end = dtstart + delta*(N + 1)

    query_params = {
        "output": { "format":  "csv" },
    }

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
