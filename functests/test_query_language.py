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


def test_read_all_in_backward_direction(dtstart, delta, N):
    """Read all data in backward direction.
    All data should be received as expected."""
    begin = dtstart + delta*N
    end = dtstart
    query = att.makequery("test", begin, end, output=dict(format='csv'))
    queryurl = "http://{0}:{1}".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))

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
            # Check values
            if timestamp != exp_ts:
                errormsg = "Invalid timestamp, expected: {0}, actual: {1}, iter: {2}".format(exp_ts, timestamp, iterations)
                raise ValueError(errormsg)
            if value != 1.0*exp_value:
                errormsg = "Invalid value, expected: {0}, actual: {1}, iter: {2}".format(exp_value, value, iterations)
                raise ValueError(errormsg)
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
    begin = dtstart + delta*N
    end = dtstart
    query_params = {
        "output": { "format":  "csv" },
        "group-by": {  "tag": "tag3" },
    }
    query = att.makequery("test", begin, end, **query_params)
    print(query)
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
        if iterations == 0:
            print(line)
        try:
            columns = line.split(',')
            tagline = columns[0].strip()
            timestamp = parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())
            # Check values
            exp_tags = expected_tags[(N-1-iterations) % len(expected_tags)]
            if tagline != exp_tags:
                errormsg = "Invalid tags, expected: {0}, actual: {1}, iter: {2}".format(exp_value, tagline, iterations)
                raise ValueError(errormsg)
            if timestamp != exp_ts:
                errormsg = "Invalid timestamp, expected: {0}, actual: {1}, iter: {2}".format(exp_ts, timestamp, iterations)
                raise ValueError(errormsg)
            if value != 1.0*exp_value:
                errormsg = "Invalid value, expected: {0}, actual: {1}, iter: {2}".format(exp_value, value, iterations)
                raise ValueError(errormsg)
            exp_ts -= delta
            exp_value -= 1
            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise

    # Check that we received all values
    print("Iterations: %d" % iterations)
    if iterations != N:
        raise ValueError("Expect {0} data points, get {1} data points".format(N, iterations))
    print("Test #2 passed")

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
        nmsgs = 1000000
        print("Sending {0} messages through TCP...".format(nmsgs))
        tags = {
            "tag1": ['A'],
            "tag2": ['B', 'C'],
            "tag3": ['D', 'E', 'F', 'G', 'H'],
        }
        for it in att.generate_messages(dt, delta, nmsgs, 'test', **tags):
            chan.send(it)
        time.sleep(5)  # wait untill all messagess will be processed

        #test_read_all_in_backward_direction(dt, delta, nmsgs)
        test_group_by_tag_in_backward_direction(dt, delta, nmsgs)
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
