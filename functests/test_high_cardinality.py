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

    nseries = 100000
    batchsz = 1000

    def get_tags():
        for ix in xrange(0, nseries):
            yield { "tag1": "A", "tag2": str(ix) }

    dt = datetime.datetime.utcnow() - (datetime.timedelta(milliseconds=1)*10)
    delta = datetime.timedelta(milliseconds=1)
    try:
        chan = att.TCPChan(HOST, TCPPORT)

        print("Sending {0} messages through TCP...".format(10*nseries))

        tags = list(get_tags())

        # Send 10 messages for each series in the set
        for it in att.generate_messages3(dt, delta, 10, 'test', tags):
            chan.send(it)

        chan.close()

        time.sleep(1)

        # kill process
        akumulid.terminate()
    except:
        traceback.print_exc()
        akumulid.terminate()
        sys.exit(1)
    finally:
        print("Server terminated")

    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    print("Server started")
    try:
        pass
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

if __name__ == '__main__':
    print(' '.join(sys.argv))
    if len(sys.argv) < 2:
        print("Not enough arguments")
        sys.exit(1)

    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
