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

def main(path):
    if not os.path.exists(path):
        print("Path {0} doesn't exists".format(path))
        sys.exit(1)
    akumulid = att.Akumulid(path)
    # delete database
    akumulid.delete_database()
    # create empty database
    akumulid.create_database()
    # start ./akumulid server
    print("Starting server...")
    akumulid.serve()
    time.sleep(5)
    try:
        # fill data in
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        host = '127.0.0.1'
        udpport = 8383
        httpport = 8181
        dt = datetime.datetime.utcnow()
        delta = datetime.timedelta(milliseconds=1)
        nmsgs = 1000000
        print("Sending {0} messages...".format(nmsgs))
        for it in att.generate_messages(dt, delta, nmsgs, 'temp', tag='test'):
            sock.sendto(it, (host, udpport))

        # check stats
        statsurl = "http://{0}:{1}/stats".format(host, httpport)
        rawstats = urllib.urlopen(statsurl).read()
        stats = json.loads(rawstats)

        # some space should be used
        volume0space = stats["volume_0"]["free_space"]
        volume1space = stats["volume_1"]["free_space"]
        if volume0space == volume1space:
            print("Test #1 failed. Nothing was written to disk, /stats:")
            print(rawstats)
            sys.exit(10)
        else:
            print("Test #1 passed")
    except Exception as err:
        print(err)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Argument required")
        sys.exit(1)
    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
