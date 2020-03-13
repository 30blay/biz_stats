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


class Period:
    def __init__(self, any_date, type):
        """
        Instantiate a Period object
        Args:
            any_date: datetime object
            type: PeriodType enum object
        """
        if not isinstance(any_date, datetime.datetime):
            raise ValueError('any_date must be of type datetime.datetime')

        if not isinstance(type, PeriodType):
            raise ValueError('Invalid period type')

        self.type = type

        if type == PeriodType.FIVEMIN:
            self.start = any_date - datetime.timedelta(minutes=any_date.minute % 5, seconds=any_date.second, microseconds=any_date.microsecond)
            self.end = self.start + datetime.timedelta(minutes=5)

        if type == PeriodType.HOUR:
            self.start = any_date.replace(minute=0, second=0, microsecond=0)
            self.end = self.start.replace(minute=59, second=59)

        if type == PeriodType.DAY:
            self.start = any_date.replace(hour=0, minute=0, second=0, microsecond=0)
            self.end = self.start.replace(hour=23, minute=59, second=59)

        if type == PeriodType.MONTH:
            self.start = any_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            self.end = last_day_of_month(any_date).replace(hour=23, minute=59, second=59)

        if type == PeriodType.YEAR:
            self.start = any_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            self.end = last_day_of_month(any_date.replace(month=12)).replace(hour=23, minute=59, second=59)

        if type == PeriodType.QUARTER:
            start_month = 3 * ((any_date.month-1) // 3) + 1
            end_month = start_month + 2
            self.start = any_date.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
            self.end = last_day_of_month(any_date.replace(month=end_month)).replace(hour=23, minute=59, second=59)

    def days(self):
        """ Get the number of days in the Period, including start and end"""
        delta = self.end - self.start
        return delta.days + 1

    def __str__(self):
        return "{} starting at {}".format(self.type.name, self.start)
