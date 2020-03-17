from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.date_utils import Period, PeriodType, last_month
from etl.Metric import *
from etl.google import export_data_to_sheet
import datetime as dt
import copy

engine = create_engine('sqlite:///warehouse.db')
warehouse = DataWarehouse(engine)

metric = AgencyUncorrectedSessions()
# metric = AgencyUniqueUsers()

jan19 = warehouse.slice_metric(
    pd.datetime(2019, 1, 3),
    pd.datetime(2019, 1, 31),
    PeriodType.DAY,
    metric)

jan20 = warehouse.slice_metric(
    pd.datetime(2020, 1, 3),
    pd.datetime(2020, 1, 31),
    PeriodType.DAY,
    metric)

mar19 = warehouse.slice_metric(
    pd.datetime(2019, 2, 25),
    pd.datetime(2019, 3, 31),
    PeriodType.DAY,
    metric)

mar20 = warehouse.slice_metric(
    pd.datetime(2020, 3, 5),
    pd.datetime.now() - dt.timedelta(days=1),
    PeriodType.DAY,
    metric)

week_ago = pd.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=7)
week_ago_hourly = warehouse.slice_metric(
    week_ago,
    pd.datetime.now() - dt.timedelta(days=7),
    PeriodType.HOUR,
    metric)

today_hourly = warehouse.slice_metric(
    pd.datetime.now().replace(hour=0, minute=0, second=0),
    pd.datetime.now(),
    PeriodType.HOUR,
    metric)

# filter out small agencies
minimum_events = 4000
jan19 = jan19[jan19.mean(axis=1) > minimum_events]

# get January change from 2019 to 2020
yoy = jan20.mean(axis=1) / jan19.mean(axis=1)

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

# add feed metadata
feeds = warehouse.get_feeds()
feeds = feeds.set_index('feed_code')[['feed_name', 'feed_location', 'country_codes']]
effect = pd.merge(feeds, effect, left_index=True, right_index=True, how='right')
effect = effect.rename(columns={
    'feed_name': 'Name',
    'feed_location': 'Municipality',
    'country_codes': 'Country code'
})
effect = effect.fillna('')

# export to google sheet
gsheet = '1d3YKhnd1F0xg-S_FifIQbsrX-FoIs4Q94ALbnuSPZWw'
effect.columns = effect.columns.map(str)
export_data_to_sheet(effect, None, gsheet, sheet='raw')
