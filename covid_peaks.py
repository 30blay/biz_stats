from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
from etl.google import export_data_to_sheet
from etl.google_sheet_getters import get_google_sheet
import datetime as dt
import pytz
from timezonefinder import TimezoneFinder
import os
from copy import copy
import socket
from covid_dashboard import get_countries, get_cities

socket.setdefaulttimeout(600)  # set timeout to 10 minutes

gsheet = '1d3YKhnd1F0xg-S_FifIQbsrX-FoIs4Q94ALbnuSPZWw'
staging = '1uaCfOpnX8s_Bf0LwIsVFUSBIWhQ34nGx41xcjyKYmdY'

cur_dir = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///{}/warehouse.db'.format(cur_dir))
warehouse = DataWarehouse(engine, amplitude_stops_changing=dt.timedelta(days=10))
metric = AgencyUncorrectedSessions()

tf = TimezoneFinder()

feeds = get_feeds().set_index('feed_code')['bounds']
feeds = pd.DataFrame(list(feeds), index=feeds.index)
feeds['lat'] = (feeds.min_lat + feeds.max_lat) / 2
feeds['lon'] = (feeds.min_lon + feeds.max_lon) / 2
feeds = feeds.dropna()
feeds['tz_name'] = [tf.timezone_at(lat=lat, lng=lon) for lat, lon in zip(feeds.lat, feeds.lon)]
feeds = feeds.dropna()

exclude = get_google_sheet(gsheet, 'exclude', header=False, range='A:A')
rename = get_google_sheet(gsheet, 'rename', range='A:C')
rename.index = rename.iloc[:, 0]


def add_aggregations(df_):
    df = copy(df_)
    df = df[~df.index.isin(exclude[0])]
    cities = get_cities(df)
    countries = get_countries(df)

    glob = df.sum().rename(('All Cities', 'All Cities'))
    feeds = get_feeds().set_index('feed_code')[['country_codes', 'feed_location']]
    df['country'] = df.index.map(feeds.country_codes)
    df['Municipality'] = df.index.map(feeds.feed_location)
    df['dashboard_name'] = df.index.map(rename['Dashboard name'])
    df = df.dropna()
    df['dashboard_name'] = df['dashboard_name'] + ' (' + df.Municipality + ')'
    df = df.reset_index().set_index(['feed_code', 'dashboard_name'])

    cities = cities.reset_index()
    cities['dashboard_name'] = cities.Municipality
    cities = cities.set_index(['Municipality', 'dashboard_name'])

    countries = countries.reset_index()
    countries['dashboard_name'] = countries.country
    countries = countries.set_index(['country', 'dashboard_name'])

    df = df.drop(columns=['country', 'Municipality', 'dashboard_name'], errors='ignore')
    df = df.append(cities, sort=False)
    df = df.append(countries, sort=False)
    df = df.append(glob)

    return df


def get_local_time(df):
    df = df.dropna()
    df.columns = [pytz.timezone('America/Toronto').localize(start) for start in df.columns]
    df.reset_index(inplace=True)

    unstacked = df.melt(id_vars='feed_code', var_name='start')
    unstacked['tz'] = unstacked.feed_code.map(feeds.tz_name)
    unstacked['corrected_start'] = [a_start.astimezone(tz).replace(tzinfo=None) for a_start, tz in zip(unstacked.start, unstacked.tz)]

    unstacked = unstacked.drop_duplicates(subset=['feed_code', 'corrected_start'])

    df = unstacked.pivot(index='feed_code', columns='corrected_start', values='value')
    # ignore 30 minute timezones
    df = df[filter(lambda c: c.minute != 30, df.columns)]
    df = df.dropna(how='all')

    return df


def get_local_hourly_slice(start, end, metric):
    df = warehouse.slice_metric(
        start - dt.timedelta(hours=18),
        end + dt.timedelta(hours=6),
        PeriodType.HOUR,
        metric)
    df = get_local_time(df)
    keepers = [date for date in df.columns if start <= date <= end]
    df = df[keepers]
    return df


def can_and_us_only(list_of_dfs):
    ret = []
    countries = get_feeds().set_index('feed_code').country_codes
    for df in list_of_dfs:
        df['country'] = df.index.map(countries)
        df = df[df.country.isin(['US', 'CAN'])]
        ret.append(df.drop(columns='country'))
    return ret


def format_time(list_of_dfs):
    ret = []
    for df in list_of_dfs:
        df.columns = df.columns.map(lambda date: date + dt.timedelta(minutes=30))
        df.columns = df.columns.map(lambda date: date.strftime('%A %H:%M'))
        ret.append(df)
    return ret


start = pd.datetime(2020, 3, 16)
end = (pd.datetime.today() - dt.timedelta(hours=24 + 7)).replace(hour=23, minute=59)

benchmark19 = warehouse.slice_metric(
    pd.datetime(2019, 2, 15),
    pd.datetime(2019, 2, 28),
    PeriodType.DAY,
    metric)
benchmark19 = add_aggregations(benchmark19)

benchmark20 = warehouse.slice_metric(
    pd.datetime(2020, 2, 15),
    pd.datetime(2020, 2, 28),
    PeriodType.DAY,
    metric)
benchmark20 = add_aggregations(benchmark20)

# filter out small cities
minimum_events = 8000
benchmark19 = benchmark19[benchmark19.mean(axis=1) > minimum_events]

yoy = benchmark20.mean(axis=1) / benchmark19.mean(axis=1)

last_year_slice = get_local_hourly_slice(
    start - dt.timedelta(days=7*53),
    end - dt.timedelta(days=7*51),
    metric)

hourly_slice = get_local_hourly_slice(
    start - dt.timedelta(days=7),
    end,
    metric)

last_year = add_aggregations(last_year_slice)
this_year = add_aggregations(hourly_slice)

last_year_yoy = last_year.multiply(yoy, axis=0)
expected = pd.DataFrame()
for date in [date for date in this_year.columns if date >= start]:
    expected[date] = (last_year_yoy[date-dt.timedelta(days=7*51)] +
                      last_year_yoy[date-dt.timedelta(days=7*52)] +
                      last_year_yoy[date-dt.timedelta(days=7*53)])\
                       / 3
expected = expected.dropna()

# add last week
this_year = this_year.reset_index().melt(id_vars=['feed_code', 'dashboard_name'], value_name='actual')
week_ago = copy(this_year)
week_ago.corrected_start = [date + dt.timedelta(days=7) for date in week_ago.corrected_start]
week_ago = week_ago.rename(columns={'actual': 'week_ago'}).drop(columns='dashboard_name')
this_year = pd.merge(this_year, week_ago, on=['feed_code', 'corrected_start'])

# add normal
normal = expected.reset_index().melt(id_vars=['feed_code', 'dashboard_name'], value_name='normal', var_name='corrected_start').drop(columns='dashboard_name')
normal['day'] = normal.corrected_start.dt.strftime('%Y-%m-%d')
hundred_percent = normal.groupby(['day', 'feed_code']).max().rename(columns={'normal': 'hundred_percent'}).drop(columns=['corrected_start'])
normal = pd.merge(normal, hundred_percent, on=['feed_code', 'day'], how='left')
peaks = pd.merge(this_year, normal, on=['feed_code', 'corrected_start'], how='right')

peaks = peaks.rename(columns={'corrected_start': 'time'})
peaks['day'] = peaks.time.dt.strftime('%Y-%m-%d')
peaks.time = peaks.time + dt.timedelta(minutes=30)
peaks.time = peaks.time.dt.hour + peaks.time.dt.minute / 60
peaks = peaks.replace([np.inf, -np.inf], np.nan)
peaks = peaks.fillna('')
peaks = peaks.set_index('feed_code')
peaks.index.rename('key')

# represent everything as a %
peaks.normal = peaks.normal / peaks.hundred_percent
peaks.actual = peaks.actual / peaks.hundred_percent
peaks.week_ago = peaks.week_ago / peaks.hundred_percent
peaks = peaks.drop(columns='hundred_percent')

export_data_to_sheet(peaks, None, gsheet, sheet='peaks', bottom_warning=False)
