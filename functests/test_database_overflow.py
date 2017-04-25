from __future__ import print_function
import os
import sys
import socket
import datetime
import time
import akumulid_test_tools as att
import json
try:
    import urllib2 as urllib
except ImportError:
    import urllib
import traceback

host = '127.0.0.1'
tcpport = 8282
httpport = 8181

# Test plan.
# - Write data until volume will be overflowed.
# - Check for overflow periodically using stats query.
# - After overflow - read all data back and validate results.


class TCPChan:
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port))

    def send(self, data):
        self.__sock.send(data)

def read_in_backward_direction(batch_size):
    """Read all data in backward direction.
    All data should be received as expected.
    Some data should be lost at the begining.
    """
    begin = datetime.datetime(year=2100, month=1, day=1)
    end = datetime.datetime(year=1970, month=1, day=1)
    query = att.makequery("temp", begin, end, output=dict(format='csv'))
    queryurl = "http://{0}:{1}/api/query".format(host, httpport)
    response = urllib.urlopen(queryurl, json.dumps(query))

    iterations = 0
    print("Test #1 - read all data in backward direction")
    pivot = None
    exp_value = None
    val_count = 0
    num_off = 0
    failcnt = 0
    for line in response:
        try:
            columns = line.split(',')
            #tagline = columns[0].strip()
            #timestamp = att.parse_timestamp(columns[1].strip())
            value = float(columns[2].strip())

            if exp_value is None and pivot is None:
                pivot = int(float(value))

            if pivot is not None and exp_value is None:
                if pivot != int(float(value)):
                    print("Off-elements count: %d" % num_off)
                    exp_value = int(float(value))
                else:
                    num_off += 1

            if exp_value:
                val_count += 1
                if float(exp_value) != value:
                    failcnt += 1
                    print("Unexpected value at {0}, actual {1}, expected {2}".format(iterations, value, exp_value))
                    exp_value = None
                    pivot = None
                    val_count = 0
                    num_off = 0

            if exp_value:
                if val_count % batch_size == 0:
                    exp_value -= 1

            if iterations % batch_size == 0:
                if iterations % (batch_size*1000) == 0:
                    print("Read {0}".format(iterations))

            iterations += 1
        except:
            print("Error at line {0}: `{1}`".format(iterations, line))
            raise

    # Check that we received all values
    if iterations == 0:
        raise ValueError("Expect {0} data points, get {1} data points".format('--', iterations))

    if failcnt != 0:
        raise ValueError("Some data was lost")

    print("Test passed")

def main(path):
    akumulid = att.create_akumulid(path)
    # delete database
    akumulid.delete_database()
    # create empty database
    akumulid.create_test_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    try:
        # fill data in
        statsurl = "http://{0}:{1}/api/stats".format(host, httpport)
        chan = TCPChan(host, tcpport)

        def get_free_space():
            rawstats = urllib.urlopen(statsurl).read()
            stats = json.loads(rawstats)
            volspace = 0
            volspace += int(stats["volume_0"]["free_space"])
            volspace += int(stats["volume_1"]["free_space"])
            volspace += int(stats["volume_2"]["free_space"])
            volspace += int(stats["volume_3"]["free_space"])
            return volspace

        print("Sending messages...")
        prevspace = get_free_space()
        batch_size = 1000
        for ix, it in enumerate(att.infinite_msg_stream(batch_size, 'temp', tag='test')):
            chan.send(it)
            if ix % 1000 == 0:
                volspace = get_free_space()
                print("{0} msgs written, free space in the database: {1}".format(ix*batch_size, volspace))
                if prevspace < volspace:  # free space increased because volume was recycled
                    print("Volume recycle occured")
                    break
                prevspace = volspace

        # Read data back if backward direction (cached values should be included)
        read_in_backward_direction(batch_size)
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
