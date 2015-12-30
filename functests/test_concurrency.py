from __future__ import print_function
import akumulid_test_tools as att
import datetime
import itertools
import json
import math
import multiprocessing
import os
import sys
import time
import traceback
try:
    from urllib2 import urlopen
except ImportError:
    from urllib import urlopen

HOST = '127.0.0.1'
TCPPORT = 8282
HTTPPORT = 8181


"""
Test plan:
    Process 1 (reader).
    - Start process 2 (writer).
    - Read all data in fwd direction in range [begin, end-window].
    Process 2 (writer).
    - Write data in range [begin, mid] in a loop.
    - Long pause.
    - Write data in range (mid, end] in a loop.
    - Exit.
"""

def writer(dt, delta, N):
    try:
        chan = att.TCPChan(HOST, TCPPORT)

        # fill data in
        print("Sending {0} messages through TCP...".format(N))
        tags = {
            "tag": ['Foo'],
        }
        print("Generating {0} messages...".format(N))
        messages = att.generate_messages(dt, delta, N, 'test', **tags)
        for it in messages:
            chan.send(it)
        print("{0} messages sent".format(N))
        time.sleep(5)
    except:
        print("Exception in writer")
        traceback.print_exc()
        sys.exit(1)

def line2tup(seq):
    for line in seq:
        columns = line.split(',')
        tagline = columns[0].strip()
        timestamp = att.parse_timestamp(columns[1].strip())
        value = float(columns[2].strip())
        yield tagline, timestamp, value

def require_continuous(seq, fn):
    """Checks that supplied sequence is ordered in a right way
    and doesn't has any gaps.
    Returns first and last elements.
    """
    first = None
    prev = None
    for it in seq:
        if first is None:
            first = it
            prev = it
            continue
        fn(it, prev)
        prev = it
    return first, prev

def reader(dtstart, delta, N):
    # Start writer process
    wproc = multiprocessing.Process(name='Writer', target=writer, args=[dtstart, delta, N])
    wproc.start()

    def cmp_tuples(lhs, rhs):
        # ignore tags
        timedelta = rhs[1] - lhs[1]
        if timedelta != delta:
            raise ValueError("Invalid timestamps, current {0}, previous {1}".format(lhs[1], rhs[1]))
        valdelta = rhs[2] - lhs[2]
        if valdelta - 1.0 > 0.000001:
            raise ValueError("Invalid value, current {0}, previous {1}".format(lhs[2], rhs[2]))

    try:
        print("Test #1 - continuous queries")
        while True:
            end = dtstart + delta*(N-1)
            begin = dtstart
            timedelta = end - begin
            query_params = {"output": { "format":  "csv" }}
            query = att.makequery("test", end, begin, **query_params)
            queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
            response = urlopen(queryurl, json.dumps(query))
            tuples = line2tup(response)
            first, last = require_continuous(tuples, cmp_tuples)
            if last is not None:
                begin = last[1]
            if first[1] == end:
                break

        print("Test passed")
    finally:
        wproc.join()

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

        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 1000000

        rproc = multiprocessing.Process(name='Reader', target=reader, args=[dt, delta, nmsgs])
        rproc.start()
        rproc.join()

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
