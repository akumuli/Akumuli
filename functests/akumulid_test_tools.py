import os
import subprocess
import datetime
try:
    import ConfigParser as ini
except ImportError:
    import configparser as ini
import os
import StringIO


def msg(timestamp, value, metric, **tags):
    timestr = timestamp.strftime('+%Y%m%dT%H%M%S.%f')
    sseries = '+{0} '.format(metric) + ' '.join(['{0}={1}'.format(key, val) for key, val in tags.iteritems()])
    strval  = '+{0}'.format(value)
    return '\r\n'.join([sseries, timestr, strval]) + '\r\n'


def generate_messages(dt, delta, N, metric_name, **kwargs):
    for i in xrange(0, N):
        tags = dict([(key, val[i % len(val)] if type(val) is list else val)
                     for key, val in kwargs.iteritems()])
        m = msg(dt, i, metric_name, **tags)
        dt = dt + delta
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

def get_config_file():
    abspath = os.path.expanduser("~/.akumulid")
    config_data = '[root]\n' + open(abspath, 'r').read()
    config = ini.RawConfigParser()
    config_fp = StringIO.StringIO(config_data)
    config.readfp(config_fp)
    return config

def get_window_width():
    config = get_config_file()
    def parse(val):
        if val.endswith('s'):
            return datetime.timedelta(seconds=int(val[:-1]))
        elif val.endswith('sec'):
            return datetime.timedelta(seconds=int(val[:-3]))
        elif val.enswith('ms'):
            return datetime.timedelta(milliseconds=int(val[:-2]))
        else:
            raise ValueError("Can't read `window` value from config")
    return parse(config.get("root", "window"))

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

