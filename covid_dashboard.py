from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.date_utils import Period, PeriodType, last_month
from etl.Metric import *
from etl.google import export_data_to_sheet
import datetime as dt
import copy
import pycountry
import os

from covid_warehouse import warehouse

metric = AgencyUncorrectedSessions()
# metric = AgencyUniqueUsers()

benchmark19 = warehouse.slice_metric(
    pd.datetime(2019, 2, 15),
    pd.datetime(2019, 2, 28),
    PeriodType.DAY,
    metric, total=True)

benchmark20 = warehouse.slice_metric(
    pd.datetime(2020, 2, 15),
    pd.datetime(2020, 2, 28),
    PeriodType.DAY,
    metric, total=True)

mar19 = warehouse.slice_metric(
    pd.datetime(2019, 2, 8),
    pd.datetime(2019, 3, 31),
    PeriodType.DAY,
    metric, total=True)

mar20 = warehouse.slice_metric(
    pd.datetime(2020, 2, 15),
    pd.datetime.now() - dt.timedelta(days=1),
    PeriodType.DAY,
    metric, total=True)

previous_hour = pd.datetime.now() - dt.timedelta(hours=1)
start_of_today = previous_hour.replace(hour=0, minute=0, second=0, microsecond=0)

week_ago = start_of_today - dt.timedelta(days=7)
week_ago_hourly = warehouse.slice_metric(
    week_ago,
    previous_hour - dt.timedelta(days=7),
    PeriodType.HOUR,
    metric, total=True)

today_hourly = warehouse.slice_metric(
    start_of_today,
    previous_hour,
    PeriodType.HOUR,
    metric, total=True)

# filter out small agencies
minimum_events = 4000
benchmark19 = benchmark19[benchmark19.mean(axis=1) > minimum_events]

# get January change from 2019 to 2020
yoy = benchmark20.mean(axis=1) / benchmark19.mean(axis=1)

mar19_yoy = mar19.multiply(yoy, axis=0)
expected_mar20 = copy.copy(mar20)
for date in mar20.columns:
    expected_mar20[date] = (mar19_yoy[date-dt.timedelta(days=7*51)] +
                            mar19_yoy[date-dt.timedelta(days=7*52)] +
                            mar19_yoy[date-dt.timedelta(days=7*53)])\
                           / 3

effect = (mar20/expected_mar20 - 1)
today_change_since_last_week = (today_hourly.mean(axis=1)/week_ago_hourly.mean(axis=1) - 1)
this_hour_change_since_last_week = (today_hourly.iloc[:, -1]/week_ago_hourly.iloc[:, -1] - 1)
effect['today so far'] = ((1+today_change_since_last_week) * (1+effect[week_ago]) - 1)
effect[today_hourly.columns[-1]] = ((1+this_hour_change_since_last_week) * (1+effect[week_ago]) - 1)
effect = effect.dropna(axis=1, how='all').dropna()

# correct using global effect up to 2/28
# global_effect = copy.copy(effect.iloc[-1, :-2].T)
# effect_stop = pd.datetime(2020, 2, 28)
# global_effect = global_effect[global_effect.index < effect_stop].mean()
# effect = effect.sub(global_effect)

# reverse, most recent on the left
effect = effect.iloc[:, ::-1]

# add feed metadata
feeds = copy.copy(warehouse.get_feeds())
feeds = feeds[feeds.country_codes != 'ZZ']
feeds.country_codes.replace('EQ', 'EC', inplace=True)
feeds = feeds.set_index('feed_code')[['feed_name', 'feed_location', 'country_codes', 'sub_country_codes']]
feeds.loc[:, 'country_codes'] = feeds.country_codes.map(lambda code: pycountry.countries.get(alpha_2=code).name)
# feeds['State'] = [pycountry.subdivisions.get(code='{}-{}'.format(c, sub_c)).name for c, sub_c in zip(feeds.country_codes, feeds.sub_country_codes)]
effect = pd.merge(feeds, effect, left_index=True, right_index=True, how='right')
effect = effect.rename(columns={
    'feed_name': 'Name',
    'feed_location': 'Municipality',
    'country_codes': 'Country',
    'sub_country_codes': 'State',
})
effect.Municipality = effect.Municipality.str.split(',').str[0]

effect = effect.replace([np.inf, -np.inf], np.nan)
effect = effect.fillna('')


# export to google sheet
gsheet = '1d3YKhnd1F0xg-S_FifIQbsrX-FoIs4Q94ALbnuSPZWw'
effect.columns = effect.columns.map(str)
export_data_to_sheet(effect, None, gsheet, sheet='raw')
