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
allevents = []

@api_test("select-events forward")
def test_select_events_forward(dtstart, delta, N):
    """Read events in forward direction"""
    nseries = 10
    begin = dtstart
    end = dtstart + delta*(N + 1)
    query = {
            "select-events": "!foo",
            "range": {
                "from": begin.strftime('%Y%m%dT%H%M%S.%f'),
                "to": end.strftime('%Y%m%dT%H%M%S.%f'),
            },
            "order-by": "time",
            "output": { "format": "csv" }
    }
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    iterations = 0
    for line in response:
        try:
            expts, expname = allevents[iterations]
            columns = line.split(',')
            timestamp = att.parse_timestamp(columns[1].strip())
            event = columns[2].lstrip().rstrip('\n')

            if timestamp != expts:
                print("Unexpected timestamp in line {0}".format(line))
                raise ValueError("Wrong timestamp {0}, expected {1}".format(str(timestamp), str(expts)))

            if expname != event:
                print("Unexpected value in line {0}".format(line))
                raise ValueError("Wrong value {0}, expected {1}".format(event, expname))

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != len(allevents):
        raise ValueError("Results incomplete, {0} received, {1} expected".format(iterations, len(allevents)))


@api_test("select-events backward")
def test_select_events_backward(dtstart, delta, N):
    """Read events in backward direction"""
    nseries = 10
    end = dtstart - delta
    begin = dtstart + delta*(N + 1)
    query = {
            "select-events": "!foo",
            "range": {
                "from": begin.strftime('%Y%m%dT%H%M%S.%f'),
                "to": end.strftime('%Y%m%dT%H%M%S.%f'),
            },
            "order-by": "time",
            "output": { "format": "csv" }
    } 
    queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
    response = urlopen(queryurl, json.dumps(query))
    iterations = 0
    for line in response:
        try:
            expts, expname = allevents[-(iterations + 1)]
            columns = line.split(',')
            timestamp = att.parse_timestamp(columns[1].strip())
            event = columns[2].lstrip().rstrip('\n')

            if timestamp != expts:
                print("Unexpected timestamp in line {0}".format(line))
                raise ValueError("Wrong timestamp {0}, expected {1}".format(str(timestamp), str(expts)))

            if expname != event:
                print("Unexpected value in line {0}".format(line))
                raise ValueError("Wrong value {0}, expected {1}".format(event, expname))

            iterations += 1
        except:
            print("Error at line: {0}".format(line))
            raise
    if iterations != len(allevents):
        raise ValueError("Results incomplete, {0} received, {1} expected".format(iterations, len(allevents)))


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
        dt = datetime.datetime.utcnow().replace(hour=0, minute=0, second=10, microsecond=0)
        delta = datetime.timedelta(seconds=1)
        nmsgs = 1000
        snames = [
            '!foo A=1 B=1',
            '!foo A=1 B=2',
            '!foo A=2 B=1',
            '!foo A=2 B=2',
        ]
        print("Sending {0} messages through TCP...".format(nmsgs*len(snames)))
        cnt = 0
        timestamp = dt
        for it in range(0, nmsgs):
            for sname in snames:
                timestr = timestamp.strftime('+%Y%m%dT%H%M%S.%f')
                event = "{0} event {1} for {2} generated".format(cnt, sname, timestr)
                msg = "+{0}\r\n+{1}\r\n+{2}\r\n".format(sname, timestr, event[:it + 1])
                allevents.append((timestamp, event[:it + 1]))
                chan.send(msg)
                cnt += 1
                timestamp = timestamp + delta
        time.sleep(5)  # wait untill all messagess will be processed

        test_select_events_forward(dt, delta, nmsgs*len(snames))
        test_select_events_backward(dt, delta, nmsgs*len(snames))

    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)
    on_exit()

if __name__ == '__main__':
    print(' '.join(sys.argv))
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
