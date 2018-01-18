'''Common utilities utilized by various packages'''

import re
import os
import imp
import sys
import pickle
import bisect
#import xml.etree.ElementTree as ET
from lxml import etree as ET
from datetime import datetime
from collections import OrderedDict, Callable
import subprocess
from functools import lru_cache

from dateutil.relativedelta import relativedelta
from dateutil.rrule import *


def iter_except(function, exception):
    '''Works like builtin 2-argument `iter()`, 
        but stops on `exception`.
    '''
    try:
        while True:
            yield function()
    except exception:
        return


def main_is_frozen():
    '''Returns True if main launched as .exe
        Returns False otherwise (run as script)
    '''
    return(hasattr(sys, "frozen") or imp.is_frozen("__main__"))


def get_main_dir1():
    if main_is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(sys.argv[0])

# def get_main_dir2():
#     if main_is_frozen():
#         return os.path.dirname(sys.executable)
#     # main_dir = os.path.dirname(sys.argv[0])
#     # if main_dir == '':
#     #     main_dir = os.getcwd()
#     # return main_dir
#     return os.path.dirname(os.path.abspath(sys.argv[0]))

def get_main_dir():
    '''Returns directory of main.py/main.exe under various circumstances'''
    if main_is_frozen():
        return os.path.dirname(sys.executable)
    if sys.argv[0] == '':
        return os.path.abspath('.')
    return os.path.dirname(os.path.abspath(sys.argv[0]))

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        #modified
        #base_path = os.path.abspath(".")
        base_path = get_main_dir()
    return os.path.join(base_path, relative_path)

def validate_machine_name(comp_name):
    acc = re.compile(r'\\\\[a-zA-Z0-9-.]+')
    mn = re.compile('[a-zA-Z0-9-.]+')
    if re.search(acc, comp_name):
        return comp_name
    else:
        try:
            base = re.search(mn, comp_name).group()
        except AttributeError:
            raise ValueError('Invalid Computer Name!')
        else:
            return r'\\' + base

def clean_amp(name):
    res = re.sub('&', '&amp;', name)
    return res

def reverse_amp(name):
    res = re.sub('&amp;', '&', name)
    return res

def get_seed():
    return datetime.now().microsecond

def format_datetime(date, output_fmt=None):
    dt = create_datetime(date)
    if output_fmt != None:
        result = dt.strftime(output_fmt)
    else:
        result = dt.strftime('%m/%d/%Y')
    return result

def format_date(date):
    #Utility function for common task, converts date string in
    # '2005-07-27' format into '07/27/2005' format
    result = datetime.strptime(date, '%Y-%m-%d').strftime('%m/%d/%Y')
    return result
    
def format_date_custom(date, input_fmt, output_fmt):
    result = datetime.strptime(date, input_fmt).strftime(output_fmt)
    return result

def get_current_date(output_fmt='%m/%d/%Y', dt=False):
    '''Return datetime or str for current date'''
    if dt is True:
        result = datetime.today()
        result = result.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        result = datetime.now().strftime(output_fmt)
    return result

def create_month_end_datetime(date_str):
    '''Utility function just to convert %Y%m 
        date string to datetime for month end date
        Returns datetime
    '''
    if len(date_str) == 6:
        dt = datetime.strptime(date_str, '%Y%m')
    elif len(date_str) == 4:
        dt = datetime.strptime(date_str, '%y%m')
    result = dt + relativedelta(months=1) - relativedelta(days=1)
    return result

def relative_date(date_str, days=None, months=None):
    dt = create_datetime(date_str)
    if days != None:
        result = dt + relativedelta(days=days)
    elif months != None:
        result = dt + relativedelta(months=months)
    return result

@lru_cache()
def create_datetime(date, input_fmt=None, from_tstamp=False):
    '''Return datetime object for date input

        Positional ARGS:
            date -- str or POSIX int or float of date

        Keyword ARGS:
            input_fmt -- format for date if str
            from_tstamp -- bool; set to True if date 
                is POSIX timestamp

        Exceptions:
            ValueError -- date conversion fails

        Utilizes input_fmt to parse string if passed 
        
        Otherwise attempts to infer string 
        format based on separator used.

        All inferred patterns assume 
        MONTH appears BEFORE DAY (USA Style)

    '''
    if isinstance(date, datetime):
        return date
    elif from_tstamp is True:
        try:
            result = datetime.fromtimestamp(int(date))
        except ValueError:
            raise
        else:
            return result
    elif type(date) is not str:
        date = str(date)
    try:
        if input_fmt != None:
            result = datetime.strptime(date, input_fmt)
        elif re.search('-', date):
            result = datetime.strptime(date, '%Y-%m-%d')
        elif re.search('/', date):
            result = datetime.strptime(date, '%m/%d/%Y')
        elif len(date) == 8:
            result = datetime.strptime(date, '%Y%m%d')
        elif len(date) == 6:
            result = datetime.strptime(date, '%Y%m')
        else:
            raise ValueError('Cannot convert date_str - format not recognized!')
    except ValueError:
        raise
    else:
        return result

def get_file_tstamp(fpath):
    '''Return datetime of Modified Date for fpath'''
    return create_datetime(os.path.getmtime(fpath), from_tstamp=True)

def convert_path_to_UNC1(fpath):
    '''Return UNC path of supplied fpath'''
    dl = fpath[0] + '$'
    result = '\\\\' + os.environ['COMPUTERNAME'] + '\\' + dl + fpath[2:]
    #result = re.sub('\\\\', '/', result)
    return result

def create_UNC_path(comp_name, fpath):
    '''Return UNC converted fpath; expects \\\\MACHINE_NAME'''
    comp_name = validate_machine_name(comp_name)
    dl = fpath[0] + '$'
    result = comp_name + '\\' + dl + fpath[2:]
    return result

def convert_path_to_UNC2(fpath):
    '''Return UNC path of supplied fpath'''
    dl = fpath[0] + '$'
    result = '//' + os.environ['COMPUTERNAME'] + '/' + dl + fpath[2:]
    result = re.sub('\\\\', '/', result)
    return result

def find_newest_file(dir_path, file_name_pattern):
    '''Return full path to most recently modified file in dir_path
        which includes match for the file_name_pattern
    '''
    file_matches = [i for i in listdir_files_fullpaths(dir_path) 
                    if re.search(file_name_pattern, i)]
    if len(file_matches) > 0:
        file_matches.sort(key=lambda x:get_file_tstamp(x), reverse=True)
        return file_matches[0]
    else:
        raise OSError('No Matches Found!')

def make_dir(dir_path):
    '''Creates directory'''
    try:
        os.makedirs(dir_path)
    except OSError:
        raise

def is_number(s):
    '''Return True if float conversion successful, else False'''
    try:
        float(s)
        return True
    except ValueError:
        return False

def is_positive(s):
    '''Return True if is_number() and positive, else False'''
    if is_number(s):
        if float(s) > 0:
            return True
        else:
            return False
    else:
        return False

def is_positive_integer(s):
    '''Return True if is_number(), positive, and int, else False'''
    if is_number(s) and is_positive(s):
        try:
            int(s)
            return True
        except ValueError:
            return False
    else:
        return False

def save_object(obj, s_file):
    '''Pickle obj to s_file path'''
    with open(s_file, 'wb') as output:
        pickle.dump(obj, output)

def load_object(s_file):
    '''Reload pickled object from s_file path'''
    with open(s_file, 'rb') as input_file:
        result = pickle.load(input_file)
    return result

def listdir_files(d):
    '''Return list of file names only in directory'''
    return [f for f in os.listdir(d) if os.path.isfile(os.path.join(d, f))]

def listdir_files_fullpaths(d):
    '''Return list of full paths for files in directory'''
    return [os.path.join(d, f) for f in os.listdir(d) if os.path.isfile(os.path.join(d, f))]

def listdir_fullpaths(d):
    '''Return list of full paths for all items in directory'''
    return [os.path.join(d, f) for f in os.listdir(d)]

def delete_files(dir_path):
    ''' Delete all files in dir_path'''
    for file in listdir_files_fullpaths(dir_path):
        try:
            os.unlink(file)
        except:
            pass

def create_backtest_dates(beg, end, freq='M', holiday_list=None):
    '''Return list of backtest dates in MM/DD/YYYY format 
        
    Positional ARGS:
        beg -- str of start date
        end -- str of end date
    
    Keyword ARGS:
        freq -- frequency of dates 
            Currently Supports: M=MONTHLY, W=WEEKLY, D=DAILY*, 
            Y=YEARLY, S=SEMI-ANNUALLY, Q=QUARTERLY
            Defaults to MONTHLY
            *DAILY only includes WEEKDAYS
        holiday_list -- list of dates to exclude
            Currently only used when freq='D'

    Exceptions:
        ValueError -- if conversion of beg/end fails
    '''
    beg_dt = create_datetime(beg)
    end_dt = create_datetime(end)
    if beg_dt == end_dt:
        return [beg_dt.strftime('%m/%d/%Y')]
    if freq == 'M':
        date_list = [date.strftime('%m/%d/%Y') for date in 
                    list(rrule(MONTHLY, dtstart=beg_dt, until=end_dt, interval=1))]
    elif freq == 'W':
        date_list = [date.strftime('%m/%d/%Y') for date in 
                    list(rrule(WEEKLY, dtstart=beg_dt, until=end_dt, interval=1))]
    elif freq == 'D':
        date_list = [date.strftime('%m/%d/%Y') for date in 
                    list(rrule(DAILY, dtstart=beg_dt, until=end_dt, interval=1, byweekday=(MO,TU,WE,TH,FR)))]
        if holiday_list != None:
            for d in holiday_list:
                try:
                    date_list.remove(d)
                except ValueError:
                    pass
    elif freq == 'S':
        date_list = [date.strftime('%m/%d/%Y') for date in 
                    list(rrule(MONTHLY, dtstart=beg_dt, until=end_dt, interval=6))]
    elif freq == 'Q':
        date_list = [date.strftime('%m/%d/%Y') for date in 
                    list[rrule(MONTHLY, dtstart=beg_dt, until=end_dt, interval=3)]]
    return date_list

def pretty_print_XML(xml_string):
    return ET.tostring(ET.fromstring(xml_string), pretty_print=True)
    
    
def open_elem(name, attribs=None):
    '''Return opening XML element str with optional attributes'''
    res = '<' + name
    if attribs != None:
        for k, v in attribs.items():
            res += ' ' + str(k) + '="' + str(v) +'"'
    res += '>\n'
    return res

def close_elem(name):
    '''Return closing XML str for name element'''
    return '</' + name + '>'

def full_elem(name, attribs):
    '''Return full XML element str with attributes'''
    res = '<' + name
    if attribs != None:
        for k, v in attribs.items():
            res += ' ' + str(k) + '="' + str(v) + '"'
    res += '/>\n'
    return res

def load_properties(prop_file):
    properties = {}
    with open(prop_file, 'r') as input_file:
        for line in input_file:
            if line.startswith('#') or line.startswith('\n'):
                pass
            else:
                tmp = line.split('=')
                properties[tmp[0]] = tmp[1].rstrip('\n')
    return properties

def gunzip(fpath):
    '''Decompress file using gunzip; return new file path'''
    try:
        result = subprocess.call('gzip -d -f %s' % fpath)
    except OSError:
        raise OSError('Error Decompressing File!')
    else:
        if result != 0:
            raise OSError('Error Decompressing File!')
        else:
            return os.path.splitext(fpath)[0]

def reverse_readline(filename, buf_size=8192):
    ''' A generator that returns the lines of a file in reverse order

        reference: http://stackoverflow.com/questions/2301789/
        read-a-file-in-reverse-roder-using-python
    '''
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] is not '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if len(lines[index]):
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment

class FileWriter(object):
    
    def __init__(self, fpath, lines=None):
        self.output = open(fpath, 'w')
        if lines != None:
            for line in lines:
                self.output.write(line)
            self.output.close()
    
    def writeOutput(self, lines):
        for line in lines:
            self.output.write(line)
    
    def close(self):
        self.output.close()

class DefaultOrderedDict(OrderedDict):
    '''OrderedDict which also provides default_factory'''
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
           not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    # 12/16/16 change self.items() to iter(self.items())
    # to allow pickling
    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, iter(self.items())

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))

    def __repr__(self):
        return 'OrderedDefaultDict(%s, %s)' % (self.default_factory,
                                               OrderedDict.__repr__(self))