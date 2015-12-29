import os
import subprocess
import socket
import datetime
try:
    import ConfigParser as ini
except ImportError:
    import configparser as ini
import os
import StringIO


def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    return datetime.datetime.strptime(ts.rstrip('0').rstrip('.'), "%Y%m%dT%H%M%S.%f")


class TCPChan:
    def __init__(self, host, port):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, port))

    def send(self, data):
        self.__sock.send(data)

    def recv(self):
        return self.__sock.recv(0x1000)

    def close(self):
        self.__sock.close()


def check_values(exp_tags, act_tags, tags_cmp_method, exp_ts, act_ts, exp_value, act_value, iterations):
    if tags_cmp_method == 'EQ':
        if act_tags != exp_tags:
            errormsg = "Invalid tags, expected: {0}, actual: {1}, iter: {2}".format(exp_tags, act_tags, iterations)
            raise ValueError(errormsg)
    elif tags_cmp_method == 'ENDS':
        if not act_tags.endswith(exp_tags):
            errormsg = "Invalid tags, expected suffix: {0}, actual: {1}, iter: {2}".format(exp_tags, act_tags, iterations)
            raise ValueError(errormsg)
    if act_ts != exp_ts:
        errormsg = "Invalid timestamp, expected: {0}, actual: {1}, iter: {2}".format(exp_ts, act_ts, iterations)
        raise ValueError(errormsg)
    if act_value != exp_value:
        errormsg = "Invalid value, expected: {0}, actual: {1}, iter: {2}".format(exp_value, act_value, iterations)
        raise ValueError(errormsg)


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

def infinite_msg_stream(batch_size, metric_name, **kwargs):
    i = 0
    dt = datetime.datetime.utcnow()
    template = '\r\n'.join(['+{2}\r\n+{0}\r\n+{1}']*batch_size) + '\r\n'
    sseries = metric_name + ' ' + ' '.join(['{0}={1}'.format(key, val) for key, val in kwargs.iteritems()])
    while True:
        dt = datetime.datetime.utcnow()
        m = template.format(dt.strftime('%Y%m%dT%H%M%S.%f'), float(i), sseries)
        yield m
        i += 1

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

    def create_test_database(self):
        """Create database in standard location"""
        cmd = os.path.join(self.__path, "akumulid")
        subprocess.call([cmd, "--CI"])

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

