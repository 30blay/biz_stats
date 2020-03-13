import datetime
import pandas as pd
import numpy as np
from functools import lru_cache
from etl.transitapp_api import get_feeds
from etl.google_sheet_getters import get_google_sheet
from etl.date_utils import days_in_month, next_month
from etl.google import export_data_to_sheet
from googleapiclient.errors import HttpError

one_sheet_to_rule_them_all = '1HxFpjjaeeVRav4WITtKzSX2EnDJ7VbpqOu9gkiKMb8U'
date_format = '%b%y'
NTD_modes_to_eliminate = ['VP', 'DR', 'DT']


def get_feed_code_to_NTDID():
    df = get_google_sheet(one_sheet_to_rule_them_all, 'input-NTD-ID')[['NTD ID', 'Feed Code']]
    df = df.rename(columns={'NTD ID': '5 digit NTD ID'})
    return df


@lru_cache()
def get_monthly_us_unlinked_trips():
    us = get_google_sheet(one_sheet_to_rule_them_all, 'input-NTD-UPT')
    NTDID_mapping = get_feed_code_to_NTDID()
    us = pd.merge(us, NTDID_mapping, on='5 digit NTD ID')
    us = us.set_index('Feed Code')

    # eliminate Vanpool, Demand Response and Demand Taxi
    us = us[~us.Modes.isin(NTD_modes_to_eliminate)]

    # keep only month columns and Feed Code
    us = us.filter(regex='[A-Z][a-z]{2}[0-9]{2}', axis='columns')

    # empty fields should be 0 not to break the sums
    us = us.replace({None: '0'})
    us = us.apply(lambda x: x.str.replace(',',''))
    us[us.columns] = us[us.columns].apply(pd.to_numeric)
    us = us.groupby('Feed Code').sum()

    # 0 trips should be None to avoid breaking the adoption metric
    us = us.replace({0: np.nan})
    return us


def get_us_yearly_unlinked_trips():
    df = get_monthly_us_unlinked_trips().T

    # group by years which are complete
    df['year'] = pd.to_datetime(df.index, format=date_format).year
    years = df.year.value_counts()
    complete_years = years.index[years == 12]
    df = df[df.year.isin(complete_years)]
    df = df.groupby('year').sum()
    df = df.T[df.T.index != '']
    return df


def get_can_dar():
    can = pd.DataFrame()
    for year in range(2017, 3000):
        try:
            df = get_google_sheet(one_sheet_to_rule_them_all, 'input-CUTA-{}'.format(year))[['Feed Code', 'Boardings']]
            df = df[df['Feed Code'] != '']
            df = df.set_index('Feed Code')
            df.Boardings = df.Boardings.str.replace(',', '').astype(int)
            can = can.reindex(can.index.union(df.index))
            for month in range(1, 13):
                date = datetime.datetime(year=year, month=month, day=1)
                can[date.strftime(date_format)] = df.Boardings / (294 * 2.25)
        except HttpError:
            break
    return can


def get_other_dar():
    ridership = get_google_sheet(one_sheet_to_rule_them_all, 'input-others-UPT').set_index('Feed Code')
    dar = pd.DataFrame()
    for year in ridership.columns.values:
        for month in range(1, 12+1):
            date = datetime.datetime(year=int(year), month=month, day=1)
            ridership[year] = pd.to_numeric(ridership[year])
            dar[date.strftime(date_format)] = ridership[year] / (294 * 2.25)
    return dar


def get_other_yearly_unlinked_trips():
    ridership = get_google_sheet(one_sheet_to_rule_them_all, 'input-others-UPT').set_index('Feed Code')
    ridership.columns = pd.to_numeric(ridership.columns)
    return ridership


def get_can_yearly(metric):
    ret = pd.DataFrame()
    for year in range(2017, 3000):
        try:
            df = get_google_sheet(one_sheet_to_rule_them_all, 'input-CUTA-{}'.format(year))[['Feed Code', metric]]
            df = df[df['Feed Code'] != '']
            df[metric] = df[metric].str.replace(',', '').str.replace('$', '')
            df[metric] = pd.to_numeric(df[metric])
            df = df.set_index('Feed Code')
            ret = ret.reindex(ret.index.union(df.index))
            ret[year] = df[metric]
        except HttpError:
            break
    return ret


def get_us_revenue():
    df = get_google_sheet(one_sheet_to_rule_them_all, 'input-NTD-MASTER')
    NTDID_mapping = get_feed_code_to_NTDID()
    df = pd.merge(df, NTDID_mapping, on='5 digit NTD ID')
    df = df.set_index('Feed Code')

    df = df[~df.Mode.isin(NTD_modes_to_eliminate)]

    df = df[' Fares FY ']

    df = df.replace({None: '0'})
    df = df.apply(lambda x: x.replace(',', ''))
    df = df.apply(lambda x: x.replace('$', ''))
    df = df.apply(lambda x: x.replace('-', '0'))
    df = pd.to_numeric(df)
    df = df.groupby('Feed Code').sum()

    return df


def predict_one_year(df):
    for i in range(12):
        ratio = (df.iloc[:, -12] + df.iloc[:, -12-12]) / (df.iloc[:, -13] + df.iloc[:, -13-12])
        last_month = datetime.datetime.strptime(df.columns[-1], date_format)
        to_predict = next_month(last_month)
        last_month_str = datetime.datetime.strftime(last_month, date_format)
        to_predict_str = datetime.datetime.strftime(to_predict, date_format)
        df[to_predict_str] = df[last_month_str] * ratio

    return df


def extend_years(df):
    """
    Predict copy the last column until current year
    :param df: a pandas DataFrane object
    :return: the extended dataframe
    """
    last_year = df.columns[-1]
    this_year = datetime.datetime.now().year
    for year in range(last_year, this_year+1):
        df[year] = df[last_year]

    return dfx


def unlinked_trips_to_dar(df):
    holidays = {
        1: 2,
        2: 1,
        3: 0,
        4: 0,
        5: 1,
        6: 0,
        7: 1,
        8: 0,
        9: 1,
        10: 1,
        11: 2,
        12: 2,
    }
    df = df.T
    dates = pd.DataFrame(index=df.index)
    dates['datetime'] = pd.to_datetime(df.index, format=date_format)
    dates['days_in_month'] = dates.datetime.map(days_in_month)
    dates['holidays'] = dates.datetime.map(lambda date: holidays[date.month])
    dates['transit_days'] = round((294/284)*(294/365)*dates.days_in_month - dates.holidays)
    unlinked_trips_per_day_per_user = 2.25
    dar = df[df.columns].divide(dates.transit_days * unlinked_trips_per_day_per_user, axis='index')
    return dar.T


def copy_last_month(df, ref):
    """ Copy the last month of df until it goes as far as ref """
    col_to_copy = df.columns[-1]
    until_month = ref.columns[-1]
    start = datetime.datetime.strptime(col_to_copy, date_format)
    end = datetime.datetime.strptime(until_month, date_format)
    month_to_predict = next_month(start)
    while month_to_predict <= end:
        col_name = month_to_predict.strftime(date_format)
        df[col_name] = df[col_to_copy]
        month_to_predict = next_month(month_to_predict)


def merge(us, can, others):
    # extend canada date to the date range of the US
    copy_last_month(can, us)
    copy_last_month(others, us)

    df = us.append(can, sort=False)
    df = df.append(others, sort=False)
    return df


def clean_dar_for_report(df):
    df = df.fillna('')
    df.columns = [datetime.datetime.strptime(col, date_format).strftime('%Y-%m') for col in df.columns]
    df = df[df.index != '']
    return df


def index_all_feeds(df):
    feeds = get_feeds()
    df = df.reindex(df.index.union(feeds.feed_code))
    return df


def update_monthly_dar():
    us = get_monthly_us_unlinked_trips()
    us = predict_one_year(us)
    us = unlinked_trips_to_dar(us)
    can = get_can_dar()
    other = get_other_dar()
    dar = merge(us, can, other)
    dar = index_all_feeds(dar)
    dar = clean_dar_for_report(dar)

    export_data_to_sheet(dar, None, one_sheet_to_rule_them_all, 'Monthly DAR')


def update_yearly_unlinked_trips():
    us = get_us_yearly_unlinked_trips()
    can = get_can_yearly('Boardings')
    other = get_other_yearly_unlinked_trips()
    trips = us.append(can, sort=False)
    trips = trips.append(other, sort=False)
    trips = index_all_feeds(trips)
    # trips = extend_years(trips)
    trips = trips.fillna('')
    export_data_to_sheet(trips, None, one_sheet_to_rule_them_all, 'Yearly unlinked trips')


def update_yearly_revenue():
    can = get_can_yearly('Total Operating Revenue')
    can['Currency'] = 'CAD'
    us = get_us_revenue()
    us = pd.DataFrame({2018: us, 'Currency': 'USD'})
    rev = can.append(us, sort=False)
    rev = index_all_feeds(rev)

    # put currency as first column
    cols = rev.columns.tolist()
    rev = rev[cols[-1:] + cols[:-1]]

    rev = extend_years(rev)
    rev = rev.fillna('')
    export_data_to_sheet(rev, None, one_sheet_to_rule_them_all, 'Yearly revenue')


update_yearly_unlinked_trips()
update_monthly_dar()
update_yearly_revenue()
