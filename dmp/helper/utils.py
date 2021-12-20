__author__ = 'phongphamhong'

import calendar
import datetime
import math
import os
import pprint
import re
import signal
import socket
import struct
import sys
import time
import traceback
from functools import wraps

import pytz
from dateutil import parser
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY, WEEKLY, MONTHLY, YEARLY
from pytz import timezone

from dmp.helper.log import logger
from dmp.helper.setting import Setting


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logger.info("[PROCESS TIME]: " + ('%r (%r, %r) %2.2f sec' % \
                                          (method.__name__, type(args), type(kw), te - ts)))
        return result

    return timed


def timeit_with_show_params(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logger.info("[PROCESS TIME]: " + '%r (%r, %r) %2.2f sec' % \
                    (method.__name__, args, kw, te - ts))
        return result

    return timed


class TimeoutError(Exception):
    pass


@timeit
def timeout(seconds=10, error_message=None):
    def decorator(func):
        def _handle_timeout(signum, frame):
            if callable(error_message):
                raise error_message()
            else:
                raise TimeoutError("Timeout after %s(s)" % seconds)

        def wrapper(*args, **kwargs):
            # disable this function on windows
            if os.name == 'nt':
                return func(*args, **kwargs)
            else:
                signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(seconds)
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                return result

        return wraps(func)(wrapper)

    return decorator


def debug_tool(*args, **kwargs):
    pprint.pprint(args)
    if "keep" not in kwargs or kwargs["keep"] == False:
        pprint.pprint("-----------------------------stop debug-------------------------------")
        sys.exit()


# works in Python 2 & 3
class Singleton(object):
    _instances = {}

    def __new__(class_, *args, **kwargs):
        if class_ not in class_._instances:
            class_._instances[class_] = super(Singleton, class_).__new__(class_, *args, **kwargs)
        return class_._instances[class_]


def intersect(a, b):
    return list(set(a) & set(b))


def parser_object_to_dict(object):
    if isinstance(object, list):
        result = []
        for k in object:
            result.append(parser_object_to_dict(k))
    elif hasattr(object, '__keylist__'):
        keylist = object['__keylist__']
        result = {}
        for key in keylist:
            item = getattr(object, key)
            data = None
            if hasattr(item, '__keylist__'):
                data = parser_object_to_dict(item)
            elif isinstance(item, list):
                data = parser_object_to_dict(item)
            else:
                data = item
            key = key.replace('.', '/')
            result[key] = data
        return result
    else:
        result = object
    return result


def compare_dicts(d1, d2, _group=True):
    result = {}
    listtype = ['__added', '__removed', '__changed_old_data', '__changed_new_data']
    for k in listtype:
        result[k] = {}

    # starting compare 2 dicts
    for k in d1.keys():
        if not d2.has_key(k):
            result['__removed'][k] = d1[k]
        else:

            if type(d1[k]) is dict:
                compare = compare_dicts(d1[k], d2[k], False)
                if compare:
                    result[k] = compare
            elif type(d1[k]) is list and type(d2[k]) is list:
                compare = compare_lists(d1[k], d2[k])
                if compare:
                    result[k] = compare
            else:
                if d1[k] != d2[k]:
                    result['__changed_old_data'][k] = d1[k]
                    result['__changed_new_data'][k] = d2[k]

    for k in d2.keys():
        if not d1.has_key(k):
            result['__added'][k] = d2[k]
    result = {k: result[k] for k in result if type(result[k]) is not dict or result[k]}
    # filter and group
    if _group and result:
        def group_result(data, root=''):
            rs = {}
            for root_key, root_item in data.iteritems():
                if root_key in listtype:
                    rs[root_key] = {}
                    if root == '':
                        rs[root_key] = root_item
                    else:
                        rs[root_key][root] = root_item
                elif type(root_item) is dict:
                    r = group_result(root_item, root_key)
                    ex = data.copy()
                    for k, v in r.iteritems():
                        if k in ex:
                            ex[k].update(v)
                        else:
                            ex[k] = v
                    rs.update({k: ex[k] for k in ex if k in listtype})
            return rs

        return group_result(result)

    return result


def compare_lists(l1, l2):
    removed = [k for k in l1 if k not in l2]
    added = [k for k in l2 if k not in l1]
    if added or removed:
        return {
            '__added': added,
            '__removed': removed,
        }
    return {}


def render_date_range(from_date, to_date, size='DAILY'):
    from_date = convert_time(from_date)
    to_date = convert_time(to_date, end_day=True)
    b = DAILY
    size = size.upper()
    if size == 'DAILY':
        b = DAILY
    elif size == 'WEEKLY':
        b = WEEKLY
    elif size == 'MONTHLY':
        b = MONTHLY
    elif size == 'YEARLY':
        b = YEARLY
    else:
        raise ValueError("size [%s] is not support!" % size)
    max = None
    for dt in rrule(b, dtstart=from_date, until=to_date):
        max = dt
        yield dt
    if max.date() != to_date.date():
        yield to_date


import copy


def generate_batches_date_range(from_date, to_date, size='DAILY'):
    from_date = convert_time(from_date)
    from_date_convert = copy.copy(from_date)
    if size == 'MONTHLY':
        from_date_convert = get_first_day_of_month(from_date)
    to_date = convert_time(to_date, end_day=True)
    date_range = list(render_date_range(from_date_convert, to_date, size=size))
    batch = []
    d_max = None
    n = 0
    len_r = len(date_range)
    for d in date_range:
        d_max = d
        lenth = len(batch)
        n += 1
        if size == 'MONTHLY':
            f_day = get_first_day_of_month(d) if n > 1 else convert_time(from_date)
            l_day = get_last_day_of_month(convert_time(d, end_day=True)) if n < len_r - 1 else to_date
            if d.month == to_date.month and d.year == to_date.year:
                yield [f_day, l_day]
                return
            yield [f_day, l_day]
            continue

        if lenth <= 0:
            batch.append(d)
        else:
            if size != 'DAILY' and d.date() == to_date.date():
                d_max = None
                n = to_date
            else:
                n = convert_time(d - datetime.timedelta(days=1), end_day=True)

            batch.append(n)
            yield batch
            batch = []
            batch.append(d)
    if d_max:
        yield [convert_time(d_max, reset_time=True), convert_time(d_max, end_day=True)]


def is_interger(number):
    return str(number).isdigit()


def is_numeric(number):
    return str(number).replace('.', '', 1).isdigit()


def access_dict_by_dot(item={}, field='', default=None, lower_key=False):
    if item and field:
        sli = field.split('.')

        if (type(item) is list and str(convert_int(field)) == field):
            index = convert_int(field)
            if len(item) > index:
                return item[index]
            return default
        else:
            if lower_key:
                sli = field.lower().split('.')
                item = {k.lower(): v for k, v in item.iteritems()}
            fs = item.get(sli[0], default)
        if len(sli) == 1:
            return fs
        del sli[0]
        return access_dict_by_dot(item=fs, field=".".join(sli), default=default, lower_key=lower_key)
    return default


def convert_int(value):
    try:
        result = int(round(convert_float(value)))
        return result
    except BaseException as e:
        return 0


def convert_long(value):
    try:
        result = convert_int(round(convert_float(value)))
        return result
    except BaseException as e:
        return 0


def convert_float(value):
    try:
        result = float(value)
        return result
    except BaseException as e:
        return 0.0


def convert_time(string_time, reset_time=False, end_day=False, convert_time_type='datetime', format='', **kwargs):
    if string_time in [None, '']:
        return None
    if format != '':
        string_time = datetime.datetime.strptime(string_time, format)
    parsing = string_time
    if type(string_time) is not datetime.datetime and type(string_time) is not datetime:
        # format YY:mm:dd
        year, month, day, hour, second, minute = convert_int(string_time[:4]), convert_int(
            string_time[5:7]), convert_int(
            string_time[8:10]), convert_int(string_time[11: 13]), convert_int(string_time[14: 16]), convert_int(
            string_time[17: 19])
        microsecond = string_time[20: 30].split('+')
        microsecond = convert_int(microsecond[0]) if microsecond else 0
        microsecond = 999999 if microsecond > 999999 else 0
        if year < 1000 or month > 12 or month < 1 or len(parsing) < 10:
            parsing = parser.parse(string_time, **kwargs)
        else:
            parsing = datetime.datetime(year, month, day, hour, second, minute, microsecond)

    if reset_time:
        parsing = parsing.replace(hour=0, minute=0, second=0, microsecond=0)
    if end_day:
        parsing = parsing.replace(hour=23, minute=59, second=59, microsecond=999999)

    if convert_time_type == 'datetime':
        return parsing
    elif convert_time_type == 'timestamp':
        return convert_timestamp(parsing)


def convert_timestamp(string_time, reset_time=False, end_day=False, big_int=True):
    if convert_int(string_time) > 0:
        return string_time
    st = convert_time(string_time, reset_time=reset_time, end_day=end_day) - convert_time('1970-01-01')
    return convert_long(int(st.total_seconds()))


def convert_timestamp_to_datetime(time_st):
    return convert_time('1970-01-01') + datetime.timedelta(seconds=time_st)


def generate_batches(iterable, batch_size_limit, callback_item=None, remove_false_value=False, remove_none=False,
                     log_object=None):
    """
    make a generate batches for a list or iterable
    :param iterable:
    :param batch_size_limit:
    :param callback_item:
    :param remove_false_value: ignore value False in list
    :param log_object: logs some message
    :return:
    """
    batch_size_limit = int(batch_size_limit)
    """
    Generator that yields lists of length size batch_size_limit containing
    objects yielded by the iterable.
    """
    batch = []
    st = time.time()
    for item in iterable:
        if callback_item:
            item = callback_item(item)
        if len(batch) == batch_size_limit:
            if log_object:
                log_object.print_log('[PROCESSING BATCH TIME]: %s' % (time.time() - st))
            yield batch
            batch = []
            st = time.time()
        if remove_none and item is None:
            continue
        else:
            batch.append(item)

    if len(batch):
        if log_object:
            log_object.print_log('[PROCESSING BATCH TIME]: %s' % (time.time() - st))
        yield batch


def current_local_iso_date(reset_time=False):
    utc = current_time_utc(reset_time)
    pst_now = utc.astimezone(pytz.timezone(Setting.LOCAL_TIME_ZONE))
    return pst_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def current_time_utc(reset_time=False):
    time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    return convert_time(time, reset_time=reset_time)


def current_time_utc_timestamp(reset_time=False):
    return convert_timestamp(current_time_utc(reset_time=reset_time))


def current_time_local(reset_time=False):
    return current_time_with_tz(tz=Setting.LOCAL_TIME_ZONE, reset_time=reset_time)


def current_time_local_timestamp(reset_time=False):
    return convert_timestamp(current_time_with_tz(tz=Setting.LOCAL_TIME_ZONE, reset_time=reset_time))


def current_time_with_tz(tz, reset_time=False):
    tz = str(tz) if tz else 'UTC'
    d = datetime.datetime.now(pytz.timezone(tz)).strftime("%Y-%m-%d %H:%M:%S.%f")

    return convert_time(d, reset_time=reset_time).astimezone(pytz.timezone(tz))


def convert_datetime_to_utc(tz, date_time, reset_time=False, check_is_dst=True):
    local = timezone(tz)
    naive = datetime.datetime.strptime(convert_time(date_time).strftime("%Y-%m-%d %H:%M:%S.%f"),
                                       "%Y-%m-%d %H:%M:%S.%f")

    if check_is_dst:
        local_dt = local.localize(naive, is_dst=None)
    else:
        try:
            local_dt = local.localize(naive, is_dst=None)
        except BaseException as e:
            local_dt = local.localize(naive, is_dst=False)
    return convert_time(local_dt.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), reset_time=reset_time)


def convert_utc_to_timzone(tz, date_time, reset_time=False):
    date_time = convert_time(date_time)
    return convert_time(
        convert_time(date_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tz)), reset_time=reset_time).strftime(
            "%Y-%m-%d %H:%M:%S.%f"))


def convert_time_tz_other_tz(date_time, from_tz, to_tz):
    # convert utc
    d = convert_datetime_to_utc(tz=from_tz, date_time=date_time)
    # convert utc to datetime that belong other timezone
    return convert_utc_to_timzone(tz=to_tz, date_time=d)


def convert_unixtime(date_time, tz=None, reset_time=False):
    if tz is None:
        tz = Setting.LOCAL_TIME_ZONE
    utc = convert_datetime_to_utc(tz=tz, date_time=date_time, reset_time=reset_time)
    return convert_timestamp(utc)


def convert_from_unixtime(unixtime, tz=None, reset_time=False):
    if tz is None:
        tz = Setting.LOCAL_TIME_ZONE
    utc = convert_timestamp_to_datetime(unixtime)
    return convert_utc_to_timzone(tz=tz, date_time=utc, reset_time=reset_time)


def ip2long(ip):
    return struct.unpack("!L", socket.inet_aton(ip))[0]


def get_int_from_string(string):
    return map(int, re.findall(r'\d+', string))


def dashrepl(matchobj):
    str6 = matchobj.group(6).strip()
    str7 = matchobj.group(7).strip()
    str5 = matchobj.group(5)
    if len(str5) == 1:
        str5 = '0' + str5

    str4 = matchobj.group(4)
    if len(str4) == 1:
        str4 = '0' + str4

    str3 = matchobj.group(3)
    if len(str3) == 1:
        str3 = '0' + str3

    str2 = matchobj.group(2)
    if len(str2) == 1:
        str2 = '0' + str2

    if str5 == '59':
        str6 = '59.999999'
    else:
        str6 = ('00:000' if str6 == '' else str6.replace(', ', ''))

    if str6 == '00.000' or str6 == '59.999999':
        str7 = ''
    else:
        str7 = ('' if str7 == '' else '.' + str7.replace(', ', ''))

    return 'ISODate("' + matchobj.group(
        1) + '-' + str2 + '-' + str3 + 'T' + str4 + ':' + str5 + ':' + str6 + str7 + 'Z")'


def convert_print(aggregate):
    str = pprint.pformat(aggregate)
    str = str.replace(' None', 'null')
    str = str.replace("u'", "'")
    str = str.replace(' True', 'true')
    str = re.sub(
        'datetime\.datetime\(([0-9]{4}), ([0-9]{1,2}), ([0-9]{1,2}), ([0-9]{1,2}), ([0-9]{1,2})(|, [0-9]{1,2})(|, [0-9]{1,7})\)',
        dashrepl, str)
    return str


def replace_array(content, array_params, a=['{', '}']):
    for key, value in array_params.iteritems():
        content = content.replace(a[0] + key + a[1], value)
    return content


def render_date_range_by_sesson_one_day(date_day, tz, to_utc=True):
    date_day = convert_time(date_day, end_day=True)
    mark_result = []
    mark_delta_time = 0
    item_range = []
    for i in range(0, 24):
        date_day = date_day.replace(hour=i)
        try:
            delta_time = (convert_datetime_to_utc(tz=tz, date_time=date_day) - date_day).total_seconds()
            if mark_delta_time == delta_time or mark_delta_time == 0:
                mark_delta_time = delta_time
                item_range.append(date_day)
            else:
                mark_delta_time = delta_time
                mark_result.append(item_range)
                item_range = []
                item_range.append(date_day)
        except BaseException as e:
            continue
    if item_range:
        mark_result.append(item_range)
    result = []

    for list_time in mark_result:
        result.append([
            list_time[0].replace(minute=00, second=00, microsecond=000000),
            list_time[-1].replace(minute=59, second=59, microsecond=999999),
        ])
    return result


def render_date_range_by_season_and_timezone(from_date, to_date, tz, to_utc=False):
    """
    split one date range to small date ranges by seasons.
    :param from_date:
    :param to_date:
    :param tz:
    :param to_utc:
    :return:
    """
    list_render = render_date_range(from_date=from_date, to_date=to_date)
    mark_delta_time = 0
    mark_result = []
    item_range = []
    for local_time in list_render:
        local_time = convert_time(local_time, reset_time=True)

        time_in_utc = convert_datetime_to_utc(date_time=local_time, tz=tz)
        delta_time = (time_in_utc - local_time).total_seconds()
        if mark_delta_time == delta_time or mark_delta_time == 0:
            mark_delta_time = delta_time
            item_range.append(local_time)
        else:
            mark_delta_time = delta_time
            # get change on one day
            session_local_time = render_date_range_by_sesson_one_day(date_day=item_range[-1], tz=tz)

            if session_local_time:
                item_range.append(session_local_time[0][-1])
            mark_result.append([item_range[0], item_range[-1]])
            item_range = [session_local_time[-1][0], local_time]

    if item_range:
        mark_result.append([item_range[0], convert_time(item_range[-1], end_day=True)])
    result = []
    for list_time in mark_result:
        st = list_time[0].replace(minute=00, second=00, microsecond=000000)
        stop = list_time[-1].replace(minute=59, second=59, microsecond=999999)
        if to_utc:
            result.append([
                convert_datetime_to_utc(date_time=st, tz=tz),
                convert_datetime_to_utc(date_time=stop, tz=tz),
            ])
        else:
            result.append([
                st,
                stop
            ])
    return result


def retry_function(function, number=3, time_sleep=10, print_log_cb=None, finish_cb=None):
    """
    run a function with retry...
    :param function:
    :return:
    """
    n = 0
    error = ""
    while True:
        n += 1

        if n >= number:
            if finish_cb:
                return finish_cb()
            raise ValueError('retry limit exceeded for function %s.\nTraceback: %s' % (function.__name__, error))
        try:
            rs = function()
            return rs
        except BaseException as e:
            trb = traceback.format_exc()
            error = trb
            if print_log_cb:
                print_log_cb(n, e)
            time.sleep(time_sleep)


def convert_value_of_dicts_by_given_define(item, define_convert):
    for k in item:
        if k in define_convert:
            if define_convert[k] in ['int', 'tinyint']:
                item[k] = convert_int(item[k])
            elif define_convert[k] == 'bigint':
                item[k] = convert_long(item[k])
            elif define_convert[k] == 'float':
                item[k] = convert_float(item[k])
            elif define_convert[k] == 'datetime':
                item[k] = convert_time(item[k])
            elif define_convert[k] == 'timestamp':
                item[k] = convert_timestamp(item[k])
    return item


def convert_value_of_dicts_by_given_define_dict(item, define_convert, type=''):
    for k, v in item.iteritems():
        key_value = k.lower() if type == 'lower' else k
        if define_convert.has_key(key_value):
            if define_convert[key_value] in ['int', 'tinyint']:
                item[k] = convert_int(item[k])
            elif define_convert[key_value] == 'bigint':
                item[k] = convert_long(item[k])
            elif define_convert[key_value] == 'float':
                item[k] = convert_float(item[k])
            elif define_convert[key_value] == 'datetime':
                item[k] = convert_time(item[k])
            elif define_convert[k] == 'timestamp':
                item[k] = convert_timestamp(item[k])
    return item


def get_last_day_of_week(day):
    dayofweek = datetime.timedelta(days=day.weekday())
    day = copy.copy(day)
    if dayofweek.days == 6:
        day = day + datetime.timedelta(days=1)
    dayofweek = datetime.timedelta(days=day.weekday())
    return day - dayofweek + datetime.timedelta(days=5)


def get_first_day_of_week(day):
    dayofweek = datetime.timedelta(days=day.weekday())
    day = copy.copy(day)
    if dayofweek.days == 6:
        day = day + datetime.timedelta(days=1)
    dayofweek = datetime.timedelta(days=day.weekday())
    return day - dayofweek - datetime.timedelta(days=1)


def get_last_day_of_month(day, formattime=''):
    d = day.replace(day=calendar.monthrange(day.year, day.month)[1])
    if formattime == 'timestamp':
        return convert_timestamp(d, reset_time=True)
    return d


def get_first_day_of_month(day):
    return convert_time('%s-%s-01' % (day.year, day.month))


def get_previous_months_by_day(day, months=1):
    t = convert_time(day)
    return t - relativedelta(months=months)


def get_next_months_by_day(day, months=1):
    t = convert_time(day)
    return t + relativedelta(months=months)


class static_property(object):
    def __init__(self, getter, setter=None):
        self._getter = getter
        self._setter = setter

    def setter(self, setter):
        self._setter = setter

    def __get__(self, obj, cls=None):
        return self._getter(cls)  # for static remove cls from the call

    def __set__(self, *args):
        return self._setter(*args)


def beauti_dict(aggregate):
    """
    make a dict is beautiful in string
    :param aggregate:
    :return:
    """

    def dashrepl(matchobj):
        str6 = matchobj.group(6).strip()
        str7 = matchobj.group(7).strip()
        str5 = matchobj.group(5)
        if len(str5) == 1:
            str5 = '0' + str5

        str4 = matchobj.group(4)
        if len(str4) == 1:
            str4 = '0' + str4

        str3 = matchobj.group(3)
        if len(str3) == 1:
            str3 = '0' + str3

        str2 = matchobj.group(2)
        if len(str2) == 1:
            str2 = '0' + str2

        if str5 == '59':
            str6 = '59.999999'
        else:
            str6 = ('00:000' if str6 == '' else str6.replace(', ', ''))

        if str6 == '00.000' or str6 == '59.999999':
            str7 = ''
        else:
            str7 = ('' if str7 == '' else '.' + str7.replace(', ', ''))

        return 'ISODate("' + matchobj.group(
            1) + '-' + str2 + '-' + str3 + 'T' + str4 + ':' + str5 + ':' + str6 + str7 + 'Z")'

    str = pprint.pformat(aggregate)
    str = str.replace(' None', 'null')
    str = str.replace("u'", "'")
    str = str.replace(' True', 'true')
    str = str.replace(' False', 'false')
    str = re.sub(
        'datetime\.datetime\(([0-9]{4}), ([0-9]{1,2}), ([0-9]{1,2}), ([0-9]{1,2}), ([0-9]{1,2})(|, [0-9]{1,2})(|, [0-9]{1,7})\)',
        dashrepl, str)
    return str


def convert_percent(value):
    try:
        result = value.replace('%', '')
        result = convert_float(result)
        return result
    except BaseException as e:
        return 0


def convert_date_to_dim_date(date_time):
    time = datetime.datetime.strftime(date_time, "%Y%m%d")
    return time


def get_quarter_from_date(date_time):
    """
    :param date_time: datetime
    :return:
    """

    month = date_time.month
    quarter = math.ceil(month / 3.)
    return "Q" + str(quarter)


def get_week_of_year(date_time):
    return date_time.isocalendar()


def replace_template(text, params):
    """
    replace template params with given param
    	text: ex: select * from {table}
    	params: ex :{table: 'table_tempƒ'}
    	return:

    """
    for k, v in params.items():
        text = text.replace('{%s}' % k, '%s' % v)
    return text
