from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
from etl.google import export_data_to_sheet
import datetime as dt
import pytz
from timezonefinder import TimezoneFinder
import os
from copy import copy


cur_dir = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///{}/warehouse.db'.format(cur_dir))
warehouse = DataWarehouse(engine, verbose=True, amplitude_stops_changing=dt.timedelta(days=20))
metric = AgencyUncorrectedSessions()

tf = TimezoneFinder()

feeds = get_feeds().set_index('feed_code')['bounds']
feeds = pd.DataFrame(list(feeds), index=feeds.index)
feeds['lat'] = (feeds.min_lat + feeds.max_lat) / 2
feeds['lon'] = (feeds.min_lon + feeds.max_lon) / 2
feeds = feeds.dropna()
feeds['tz_name'] = [tf.timezone_at(lat=lat, lng=lon) for lat, lon in zip(feeds.lat, feeds.lon)]
feeds = feeds.dropna()


def add_aggregations(df):
    glob = df.sum().rename('All Cities')
    feeds = get_feeds().set_index('feed_code')[['country_codes', 'feed_location']]
    df['country'] = df.index.map(feeds.country_codes)
    df['Municipality'] = df.index.map(feeds.feed_location)
    countries = df.groupby("country").sum()
    cities = df.groupby("Municipality").sum()
    df = df.append(countries[countries.index.isin(['CA', 'US', 'FR'])], sort=False)
    df = df.rename(index={'CA': 'Canada', 'US': 'United States', 'FR': 'France'})
    df = df.drop(columns=['country', 'Municipality'], errors='ignore')
    df = df.append(glob)

    cities_mode = True
    if cities_mode:
        df = cities
        df = df.append(glob)
    return df


def get_local_time(df):
    df = df.dropna()
    df.columns = [pytz.timezone('America/Toronto').localize(start) for start in df.columns]
    df.reset_index(inplace=True)

    unstacked = df.melt(id_vars='feed_code', var_name='start')
    unstacked['tz'] = unstacked.feed_code.map(feeds.tz_name)
    unstacked['corrected_start'] = [a_start.astimezone(tz).replace(tzinfo=None) for a_start, tz in zip(unstacked.start, unstacked.tz)]

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


normal_slice = get_local_hourly_slice(
    pd.datetime(2020, 2, 23),
    pd.datetime(2020, 2, 29, 23),
    metric)


hourly_slice = get_local_hourly_slice(
    pd.datetime(2020, 3, 9),
    (pd.datetime.today() - dt.timedelta(days=2)).replace(hour=23, minute=59),
    metric)

normal = add_aggregations(normal_slice)
hourly = add_aggregations(hourly_slice)

minimum_events_per_hour = 200
normal = normal[normal.mean(axis=1) > minimum_events_per_hour]

# all last week
hourly = hourly.reset_index().melt(id_vars='Municipality', value_name='actual')
week_ago = copy(hourly)
week_ago.corrected_start = [date + dt.timedelta(days=7) for date in week_ago.corrected_start]
week_ago = week_ago.rename(columns={'actual': 'week_ago'})
hourly = pd.merge(hourly, week_ago, on=['Municipality', 'corrected_start'])

# add normal
normal = normal.reset_index().melt(id_vars='Municipality', value_name='normal')
normal['weekday'] = normal.corrected_start.map(lambda date: date.strftime('%A %H:%M'))
normal = normal.drop(columns='corrected_start')
hourly['weekday'] = hourly.corrected_start.map(lambda date: date.strftime('%A %H:%M'))
peaks = pd.merge(hourly, normal, on=['Municipality', 'weekday']).drop(columns='weekday')

peaks = peaks.rename(columns={'corrected_start': 'time'})
peaks['day'] = peaks.time.dt.strftime('%Y-%m-%d')
peaks.time = peaks.time + dt.timedelta(minutes=30)
peaks.time = peaks.time.dt.hour + peaks.time.dt.minute / 60
peaks = peaks.replace([np.inf, -np.inf], np.nan)
peaks = peaks.fillna('')
peaks = peaks.set_index('Municipality')


# export to google sheet
gsheet = '1d3YKhnd1F0xg-S_FifIQbsrX-FoIs4Q94ALbnuSPZWw'
staging = '1uaCfOpnX8s_Bf0LwIsVFUSBIWhQ34nGx41xcjyKYmdY'
export_data_to_sheet(peaks, None, staging, sheet='peaks', bottom_warning=False)
