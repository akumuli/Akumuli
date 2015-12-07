import os
import subprocess
import datetime


def msg(timestamp, value, metric, **tags):
    timestr = timestamp.strftime('+%Y%m%dT%H%M%S.%f')
    sseries = '+{0} '.format(metric) + ' '.join(['{0}={1}'.format(key, val) for key, val in tags.iteritems()])
    strval  = '+{0}'.format(value)
    return '\r\n'.join([sseries, timestr, strval]) + '\r\n'


def generate_messages(dt, delta, N, metric_name, **kwargs):
    for i in xrange(0, N):
        dt = dt + delta
        tags = dict([(key, val[i % len(val)] if type(val) is list else val)
                     for key, val in kwargs.iteritems()])
        m = msg(dt, i, metric_name, **tags)
        if i == (N-1):
            print(m)
        yield m

def makequery(metric, begin, end, **kwargs):
    query = {
            "metric": metric,
            "range": {
                "from": begin.strftime('%Y%m%dT%H%M%S.%f'),
                "to": end.strftime('%Y%m%dT%H%M%S.%f'),
                }
            }
    query.update(**kwargs)
    return query


class Akumulid:
    """akumulid daemon instance"""
    def __init__(self, path):
        self.__path = path

    def create_database(self):
        """Create database in standard location"""
        cmd = os.path.join(self.__path, "akumulid")
        subprocess.call([cmd, "--create"])

    def delete_database(self):
        """Remove database from standard location"""
        cmd = os.path.join(self.__path, "akumulid")
        subprocess.call([cmd, "--delete"])

    def serve(self):
        cmd = os.path.join(self.__path, "akumulid")
        self.__process = subprocess.Popen([cmd])

    def stop(self):
        self.__process.send_signal(subprocess.signal.SIGINT)
        
    def terminate(self):
        self.__process.terminate()

