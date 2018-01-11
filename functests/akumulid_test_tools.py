from __future__ import print_function
import os
import sys
import subprocess
import socket
import datetime
import random
import traceback
try:
    import ConfigParser as ini
except ImportError:
    import configparser as ini
import StringIO
import time
from functools import wraps


def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    try:
        return datetime.datetime.strptime(ts.rstrip('0').rstrip('.'), "%Y%m%dT%H%M%S.%f")
    except ValueError:
        return datetime.datetime.strptime(ts.rstrip('0').rstrip('.'), "%Y%m%dT%H%M%S")


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

def bulk_msg(ts, measurements, **tags):
    ncol = len(measurements)
    metric = "|".join(measurements.keys())
    sname = "+" + metric + ' ' + ' '.join(['{0}={1}'.format(key, val) for key, val in tags.iteritems()])
    timestr = ts.strftime('+%Y%m%dT%H%M%S.%f')
    header = "*{0}".format(ncol)
    lines = [sname, timestr, header]
    for metric, val in measurements.iteritems():
        lines.append("+{0}".format(val))
    return '\r\n'.join(lines) + '\r\n'

def generate_bulk_messages(dt, delta, N, metric_names, **kwargs):
    for i in xrange(0, N):
        tags = dict([(key, val[i % len(val)] if type(val) is list else val)
                     for key, val in kwargs.iteritems()])
        values = [(name, i + i*(ix*10)) for ix, name in enumerate(metric_names)]
        m = bulk_msg(dt, dict(values), **tags)
        dt = dt + delta
        yield m

def generate_messages(dt, delta, N, metric_name, **kwargs):
    for i in xrange(0, N):
        tags = dict([(key, val[i % len(val)] if type(val) is list else val)
                     for key, val in kwargs.iteritems()])
        m = msg(dt, i, metric_name, **tags)
        dt = dt + delta
        yield m


def generate_messages2(dt, delta, N, metric_name, value_gen, **kwargs):
    for i in xrange(0, N):
        tags = dict([(key, val[i % len(val)] if type(val) is list else val)
                     for key, val in kwargs.iteritems()])
        m = msg(dt, value_gen(i), metric_name, **tags)
        dt = dt + delta
        yield m

def generate_messages3(dt, delta, N, metric_name, tagslist):
    """Each series will get the same set of timestamps"""
    for i in xrange(0, N):
        for tags in tagslist:
            m = msg(dt, i, metric_name, **tags)
            yield m
        dt = dt + delta

def infinite_msg_stream(batch_size, metric_name, **kwargs):
    i = 0
    template = '\r\n'.join(['+{2}\r\n+{0}\r\n+{1}']*batch_size) + '\r\n'
    sseries = metric_name + ' ' + ' '.join(['{0}={1}'.format(key, val) for key, val in kwargs.iteritems()])
    while True:
        dt = datetime.datetime.utcnow()
        m = template.format(dt.strftime('%Y%m%dT%H%M%S.%f'), float(i), sseries)
        yield m
        i += 1

def make_select_query(metric, begin, end, **kwargs):
    query = {
            "select": metric,
            "range": {
                "from": begin.strftime('%Y%m%dT%H%M%S.%f'),
                "to": end.strftime('%Y%m%dT%H%M%S.%f'),
                }
            }
    query.update(**kwargs)
    return query

makequery = make_select_query

def make_aggregate_query(metric, begin, end, func, **kwargs):
    query = {
            "aggregate": { metric: func },
            "range": {
                "from": begin.strftime('%Y%m%dT%H%M%S.%f'),
                "to": end.strftime('%Y%m%dT%H%M%S.%f'),
                }
            }
    query.update(**kwargs)
    return query

def make_group_aggregate_query(metric, begin, end, func, step, **kwargs):
    if type(func) is not list:
        raise ValueError("`func` should be a list")
    query = {
            "group-aggregate": { 
                "metric":metric,
                "func": func,
                "step": step
            },
            "range": {
                "from": begin.strftime('%Y%m%dT%H%M%S.%f'),
                "to": end.strftime('%Y%m%dT%H%M%S.%f'),
                }
            }
    query.update(**kwargs)
    return query

def make_join_query(metrics, begin, end, **kwargs):
    query = {
            "join": metrics,
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
        ts = datetime.datetime.now()
        print("Akumulid launch time: " + ts.strftime("%Y-%m-%d %H:%M:%S,%f"))
        cmd = os.path.join(self.__path, "akumulid")
        self.__process = subprocess.Popen([cmd])

    def stop(self):
        self.__process.send_signal(subprocess.signal.SIGINT)
        
    def terminate(self):
        self.__process.kill()

class FakeAkumulid:
    """akumulid daemon instance"""
    def __init__(self):
        pass

    def create_database(self):
        pass

    def create_test_database(self):
        pass

    def delete_database(self):
        pass

    def serve(self):
        pass

    def stop(self):
        pass
        
    def terminate(self):
        pass

def create_akumulid(path):
    if path == "DEBUG":
        return FakeAkumulid()
    if not os.path.exists(path):
        print("Path {0} doesn't exists".format(path))
        sys.exit(1)
    return Akumulid(path)

def set_log_path(path):
    key = 'log4j.appender.file.filename'
    edit_config_file(key, path)

def set_nvolumes(num):
    key = 'nvolumes'
    edit_config_file(key, str(num))

def set_volume_size(size):
    key = 'volume_size'
    edit_config_file(key, size)

def edit_config_file(key, value):
    abspath = os.path.expanduser("~/.akumulid")
    lines = []
    success = False
    with open(abspath, 'r') as configfile:
        for line in configfile:
            if line.startswith(key):
                lines.append(key + '=' + value + '\n')
                success = True
            else:
                lines.append(line)

    if not success:
        lines.append(key + '=' + value + '\n')

    with open(abspath, 'w') as configfile:
        for line in lines:
            configfile.write(line)

# Globals to count test runs
g_test_run = 1
g_num_fail = 0

def api_test(test_name):
    def decorator(func):
        def wrapper(*pos, **kv):
            global g_test_run
            global g_num_fail
            n = g_test_run
            g_test_run += 1
            ts = datetime.datetime.now()
            ts = ts.strftime("%Y-%m-%d %H:%M:%S,%f")
            print("Test #{0} - {1} / {2}".format(n, test_name, ts))
            try:
                func(*pos, **kv)
                print("Test #{0} passed".format(n))
            except ValueError as e:
                print("Test #{0} failed: {1}".format(n, e))
                g_num_fail += 1
                traceback.print_exc()
        return wrapper
    return decorator

def on_exit():
    global g_num_fail
    if g_num_fail != 0:
        print("{0} tests failed".format(g_num_fail))
        sys.exit(1)

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

if __name__=='__main__':
    if len(sys.argv) < 2:
        print("Command required: commands available:\n" +
              " set_log_path <path>\n")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == 'set_log_path':
        if len(sys.argv) < 3:
            print("Invalid command, arg required - `set_log_path <path>`")
            sys.exit(1)
        else:
            path = sys.argv[2]
            set_log_path(path)
    elif cmd == 'set_nvolumes':
        if len(sys.argv) < 3:
            print("Invalid command, arg required - `set_nvolumes <num>`")
            sys.exit(1)
        else:
            num = sys.argv[2]
            set_nvolumes(num)
    elif cmd == 'set_volume_size':
        if len(sys.argv) < 3:
            print("Invalid command, arg required - `set_volume_size <size>`")
            sys.exit(1)
        else:
            size = sys.argv[2]
            set_volume_size(size)
    else:
        print("Unknown command " + cmd)
        sys.exit(1)
