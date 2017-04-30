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

        # Case 1
        invalid_sample = "+cpuload host=machine1\\r\\n:1418224205000000000\\r\\r+25.0\\r\\n\n"  # reportd in issue#173
        chan.send(invalid_sample)
        #time.sleep(1)  # wait untill all messagess will be processed
        query = {"select":"cpuload","range": {"from":1418224205000000000, "to":1418224505000000000}}
        queryurl = "http://{0}:{1}/api/query".format(HOST, HTTPPORT)
        response = urlopen(queryurl, json.dumps(query))
        # response should be empty
        for line in response:
            print("Unexpected response: {0}".format(line))
            raise ValueError("Unexpected response")
        err = chan.recv()
        print(err)
        if not err.startswith("-PARSER"):
            raise ValueError("Error message expected")
        chan.close()

        # Case 2
        chan = att.TCPChan(HOST, TCPPORT)
        invalid_sample = "+cpuload host=machine2\r\n:1418224205000000000\r\n+25.0"
        chan.send(invalid_sample)
        time.sleep(1)
        response = urlopen(queryurl, json.dumps(query))
        # response should be empty
        for line in response:
            print("Unexpected response: {0}".format(line))
            raise ValueError("Unexpected response")
        # No error message expected because the write is incomplete
        chan.close()
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
