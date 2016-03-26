"""Sends invalid series id without set of tags.
As result akumulid shouldn't crash but send some error
information back to client. This should be transformed
into automated test.
"""
from socket import *

EP = ("127.0.0.1", 8282)
MSG = '+metric\r\n:123\r\n+5.0'

def send():
    s = socket(AF_INET, SOCK_STREAM)
    s.connect(EP)
    s.send(MSG)
    print s.recv(1024)  # this will hang on success but will
                        # return error message in our case
                        # because we're sending bad data
    s.close()

if __name__ == '__main__':
    send()
