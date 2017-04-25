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

def test_sax_in_backward_direction(dtstart, delta, N):
    begin = dtstart + delta*N
    end = dtstart
    query_params = {
            "sample": [{          "name": "sax", 
                         "alphabet_size": "5", 
                          "window_width": "10" }],
            "output":  {        "format": "csv" },
            "group-by":{          "time": "1ms" },
    }
    query = att.makequery("test", begin, end, **query_params)
    print(query)
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    iterations = 0
    print("Test")
    expected_tags = [
        "tag3=H",
        "tag3=G",
        "tag3=F",
        "tag3=E",
        "tag3=D",
    ]
    exp_value = "aabbccddee"
    for line in response:
        print(line)
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            value = columns[2].strip()

            exp_tags = expected_tags[iterations % len(expected_tags)]
            if value != exp_value:
                raise ValueError("Expected {0}, actual {1}".format(exp_value, value))
            if not tagline.endswith(exp_tags):
                raise ValueError("Expected {0}, actual {1}".format(exp_tags, tagline))

            if (iterations + 1) % 50 == 0:
                exp_ts -= datetime.timedelta(seconds=5)
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    if iterations != N/10:
        raise ValueError("Expect {0} data points, get {1} data points".format(N/10, iterations))
    print("Test passed")

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
        def getval(ix):
            return float(ix % (nmsgs*10))
        for it in att.generate_messages2(dt, delta, nmsgs, 'test', getval, **tags):
            chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed

        test_sax_in_backward_direction(dt, delta, nmsgs)
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
