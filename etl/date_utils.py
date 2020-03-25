import datetime
import pandas as pd
from enum import Enum


def next_month(any_day):
    any_day_next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    first_day_next_month = any_day_next_month.replace(day=1)
    return first_day_next_month


def last_month(any_day):
    day_last_month = any_day.replace(day=1) - datetime.timedelta(days=1)
    first_day_last_month = day_last_month.replace(day=1)
    return first_day_last_month


def last_day_of_month(any_day):
    """ example :
    last_day_of_month(2019-12-15) will return 2019-12-31"""
    return next_month(any_day) - datetime.timedelta(days=1)


def days_in_month(any_day):
    """ Returns the number of days of the month containing any_day"""
    return last_day_of_month(any_day).day


def weekday_avg(df):
    """
    Average a metric on weekdays only (monday through friday)
    :param df: a pandas DataFrame
    :return: a Series object
    """
    df = df.transpose()
    df.index = pd.to_datetime(df.index)
    avg = df[df.index.dayofweek < 5].mean()
    return avg


class PeriodType(Enum):
    FIVEMIN = 1
    HOUR = 2
    DAY = 3
    MONTH = 4
    QUARTER = 5
    YEAR = 6
