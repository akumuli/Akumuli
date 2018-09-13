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

class UDPChan:
    def __init__(self, host, port):
        self.__host = host
        self.__port = port
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    def send(self, data):
        self.__sock.sendto(data, (self.__host, self.__port))

def main(path):
    akumulid = att.create_akumulid(path)
    # delete database
    akumulid.delete_database()
    # create empty database
    akumulid.create_database()
    # start ./akumulid server
    try:
        print("Starting server...")
        akumulid.serve()
        time.sleep(5)
        
        host = '127.0.0.1'
        udpport = 8383
        chan = UDPChan(host, udpport)
        nmsgs = 1000
        for it in range(0, nmsgs):
            # Send datagram with signle line that should create new series name
            msg = "+metric tag={0}\r\n".format(it)
            chan.send(msg)
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

    try:
        # Server should start correctly
        print("Starting server...")
        akumulid.serve()
        time.sleep(5)

        # TODO: read names
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("Stopping server...")
        akumulid.stop()
        time.sleep(5)

if __name__ == '__main__':
    print(' '.join(sys.argv))
    if len(sys.argv) != 2:
        print("Not enough arguments")
        sys.exit(1)
    main(sys.argv[1])
else:
    raise ImportError("This module shouldn't be imported")
