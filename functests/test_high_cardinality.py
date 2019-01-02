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

def main(path):
    akumulid = att.create_akumulid(path)
    # Reset database
    akumulid.delete_database()
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)

    nseries = 1000000

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
        for ix, it in enumerate(att.generate_messages5(dt, delta, 10, 'test', tags)):
            chan.send(it)
            if ix % 100000 == 0:
                print("{0} series created".format(ix))

        chan.close()

        time.sleep(5)

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
