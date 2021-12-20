#!/usr/bin/env python
# -*- coding: utf-8 -*-
# !/usr/bin/python
#
# Copyright 11/9/18 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# # set enviroment in dev mode
__author__ = 'phongphamhong'

import calendar
import copy
import datetime
import math
import time
import traceback

import pytz
from dateutil import parser
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY, WEEKLY, MONTHLY, YEARLY
from pytz import timezone

from dmp.helper.setting import Setting


def render_date_range(from_date, to_date, size='DAILY'):
    from_date = convert_time(from_date)
    to_date = convert_time(to_date, end_day=True)
    b = DAILY
    size = size.strip().upper()
    if size == 'DAILY':
        b = DAILY
    elif size == 'WEEKLY':
        b = WEEKLY
    elif size == 'MONTHLY':
        b = MONTHLY
    elif size == 'YEARLY':
        b = YEARLY
    max = None
    for dt in rrule(b, dtstart=from_date, until=to_date):
        max = dt
        yield dt
    if max.date() != to_date.date():
        yield to_date


def convert_timestamp(string_time, reset_time=False, end_day=False, big_int=True):
    if convert_int(string_time) > 0:
        return string_time
    st = convert_time(string_time, reset_time=reset_time, end_day=end_day) - convert_time('1970-01-01')
    return convert_long(int(st.total_seconds()))


def convert_timestamp_to_datetime(time_st):
    return convert_time('1970-01-01') + datetime.timedelta(seconds=time_st)


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


def current_local_iso_date(reset_time=False):
    utc = current_time_utc(reset_time)
    pst_now = utc.astimezone(pytz.timezone(Setting.LOCAL_TIME_ZONE))
    return pst_now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def current_time_utc(reset_time=False, end_day=False):
    time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    return convert_time(time, reset_time=reset_time, end_day=end_day)


def current_time_utc_timestamp(reset_time=False):
    return convert_timestamp(current_time_utc(reset_time=reset_time))


def current_time_local(reset_time=False, end_day=False):
    return current_time_with_tz(tz=Setting.LOCAL_TIME_ZONE, reset_time=reset_time, end_day=end_day)


def current_time_local_timestamp(reset_time=False):
    return convert_timestamp(current_time_with_tz(tz=Setting.LOCAL_TIME_ZONE, reset_time=reset_time))


def current_time_with_tz(tz, reset_time=False, end_day=False):
    tz = str(tz) if tz else 'UTC'
    d = datetime.datetime.now(pytz.timezone(tz)).strftime("%Y-%m-%d %H:%M:%S.%f")
    return convert_time(d, reset_time=reset_time, end_day=end_day).astimezone(pytz.timezone(tz))


def current_unixtime():
    return int(time.time())


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

        from_date:
        to_date:
        tz:
        to_utc:
        return:

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

        function:
        return:

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


def get_first_day_of_year(day, formattime=''):
    d = datetime.datetime(day.year, 1, 1)
    if formattime == 'timestamp':
        return convert_timestamp(d, reset_time=True)
    return d


def get_last_day_of_year(day, formattime=''):
    d = datetime.datetime(day.year, 12, 31)
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


def convert_date_to_dim_date(date_time):
    time = datetime.datetime.strftime(date_time, "%Y%m%d")
    return time


def get_quarter_from_date(date_time):
    '''
        date_time: datetime
        return:

    '''

    month = date_time.month
    quarter = math.ceil(month / 3.)
    return "Q" + str(quarter)
