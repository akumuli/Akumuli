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
import urllib

HOST = '127.0.0.1'
TCPPORT = 8282
HTTPPORT = 8181
NSERIES = 1000000

def test_metadata(metric, taglist):
    # generate all possible series
    expected_series = []
    for it in taglist:
        series = "{0} {1}".format(metric, " ".join(["{0}={1}".format(tag, value) for tag, value in it.items()]))
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

def main(path):
    akumulid = att.create_akumulid(path)
    # Reset database
    akumulid.delete_database()
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)

    def get_tags():
        for ix in xrange(0, NSERIES):
            yield { "tag1": "A", "tag2": str(ix) }

    tags = list(get_tags())

    dt = datetime.datetime.utcnow() - (datetime.timedelta(milliseconds=1)*10)
    delta = datetime.timedelta(milliseconds=1)
    try:
        chan = att.TCPChan(HOST, TCPPORT)

        print("Sending {0} messages through TCP...".format(10*NSERIES))

        # Send 10 messages for each series in the set
        for ix, it in enumerate(att.generate_messages5(dt, delta, 10, 'test', tags)):
            chan.send(it)
            if ix % 100000 == 0:
                print("{0} series created".format(ix))

        chan.close()

        time.sleep(15)

        # kill process
        akumulid.terminate()
    except:
        traceback.print_exc()
        akumulid.terminate()
        sys.exit(1)
    finally:
        print("Server terminated")

    print("Starting recovery...")
    akumulid.serve()
    while True:
        try:
            # Wait until server will respond to stas query
            # which mean that the recovery is completed.
            statsurl = "http://{0}:{1}/api/stats".format(HOST, HTTPPORT)
            _ = urllib.urlopen(statsurl).read()
        except:
            time.sleep(1)
            continue
        break
    print("Recovery completed")
    try:
        test_metadata("test", tags)
    finally:
        print("Stopping server...")
        akumulid.terminate()
        time.sleep(5)

if __name__ == '__main__':
    print(' '.join(sys.argv))
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)

    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
