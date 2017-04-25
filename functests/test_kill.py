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

def test_read_all(exp_tags, dtstart, delta, N):
    """Read all series one by one in backward direction.
    All data should be received as expected."""
    for tags in exp_tags:
        begin = dtstart + delta*(N-1)
        end = dtstart
        query_params = {
            "output": { "format":  "csv" },
            "where": tags
        }
        query = att.makequery("test", begin, end, **query_params)
        queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
        response = urlopen(queryurl, json.dumps(query))

        exp_ts = None
        print("Test - read all data in backward direction")
        prev_line = ''
        iterations = 0
        for line in response:
            try:
                columns = line.split(',')
                timestamp = att.parse_timestamp(columns[1].strip())
                if exp_ts is None:
                    exp_ts = timestamp

                if exp_ts and exp_ts != timestamp:
                    raise ValueError("Invalid timestamp at {0}, expected {1}, actual {2}".format(iterations, exp_ts, timestamp))

                exp_ts -= delta
                iterations += 1
                prev_line = line
            except ValueError as err:
                print(err)
                raise

        # Check that we received all values
        if iterations == 0:
            raise ValueError("Unable to read any data")

    print("Test passed")


def main(path):
    if not os.path.exists(path):
        print("Path {0} doesn't exists".format(path))
        sys.exit(1)

    akumulid = att.Akumulid(path)
    # Reset database
    akumulid.delete_database()
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)

    nmsgs = 100000
    tags = [
        {"tag3": "D", "tag2": "B", "tag1": "A"},
        {"tag3": "E", "tag2": "B", "tag1": "A"},
        {"tag3": "F", "tag2": "B", "tag1": "A"},
        {"tag3": "G", "tag2": "B", "tag1": "A"},
        {"tag3": "H", "tag2": "B", "tag1": "A"},
        {"tag3": "D", "tag2": "C", "tag1": "A"},
        {"tag3": "E", "tag2": "C", "tag1": "A"},
        {"tag3": "F", "tag2": "C", "tag1": "A"},
        {"tag3": "G", "tag2": "C", "tag1": "A"},
        {"tag3": "H", "tag2": "C", "tag1": "A"},
    ]
    dt = datetime.datetime.utcnow() - (datetime.timedelta(milliseconds=1)*nmsgs)
    delta = datetime.timedelta(milliseconds=1)
    try:
        chan = att.TCPChan(HOST, TCPPORT)

        # fill data in
        print("Sending {0} messages through TCP...".format(nmsgs))

        for it in att.generate_messages3(dt, delta, nmsgs, 'test', tags):
            chan.send(it)

        # kill process
        akumulid.terminate()
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("Server terminated")

    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    print("Server started")
    try:
        test_read_all(tags, dt, delta, nmsgs)

        # Try to write some data
        chan = att.TCPChan(HOST, TCPPORT)
        dt = datetime.datetime.utcnow()

        print("Sending {0} messages through TCP...".format(nmsgs))
        for it in att.generate_messages3(dt, delta, nmsgs, 'test', tags):
            chan.send(it)

        time.sleep(5)  # wait untill all messagess will be processed
        print("Trying to close channel")
        chan.close()

        test_read_all(tags, dt, delta, nmsgs)
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
