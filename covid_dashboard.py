from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
from etl.google import export_data_to_sheet
import datetime as dt
import copy
import pycountry
import os
import re


cur_dir = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///{}/warehouse.db'.format(cur_dir))

warehouse = DataWarehouse(engine, amplitude_stops_changing=dt.timedelta(days=20))


def get_cities(df_):
    df = copy.copy(df_)
    feeds = get_feeds().set_index('feed_code')[['country_codes', 'feed_location']]
    df['Municipality'] = df.index.map(feeds.feed_location)
    cities = df.groupby("Municipality").sum()
    cities = cities[cities.index != 'California']
    cities = cities.rename(index={
        'SF Bay Area': 'San Francisco Bay Area',
        'NYC': 'New York City',
    })
    return cities


def get_countries(df_):
    df = copy.copy(df_)
    feeds = get_feeds().set_index('feed_code')[['country_codes', 'feed_location']]
    df['country'] = df.index.map(feeds.country_codes)
    countries = df.groupby("country").sum()
    countries = countries[countries.index.isin(['CA', 'US', 'FR', 'GB', 'AU', 'NZ'])]
    countries = countries.rename(index={
        'CA': 'Canada',
        'US': 'United States',
        'FR': 'France',
        'GB': 'United Kingdom',
        'AU': 'Australia',
        'NZ': 'New Zealand',
    })
    return countries


if __name__ == "__main__":
    for cities_mode in [False]:
        def add_aggregations(df):
            glob = df.sum().rename('Global')

            cities = get_cities(df)
            countries = get_countries(df)

            df = df.append(countries, sort=False)
            df = df.append(glob, sort=False)

            if cities_mode:
                df = cities
            return df


        metric = AgencyUncorrectedSessions()
        # metric = AgencyUniqueUsers()

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

        mar19 = warehouse.slice_metric(
            pd.datetime(2019, 2, 8),
            pd.datetime(2019, 10, 30),
            PeriodType.DAY,
            metric)
        mar19 = add_aggregations(mar19)

        mar20 = warehouse.slice_metric(
            pd.datetime(2020, 2, 15),
            pd.datetime.now() - dt.timedelta(days=1),
            PeriodType.DAY,
            metric)
        mar20 = add_aggregations(mar20)

        previous_hour = pd.datetime.now() - dt.timedelta(hours=1)
        start_of_today = previous_hour.replace(hour=0, minute=0, second=0, microsecond=0)

        week_ago = start_of_today - dt.timedelta(days=7)
        week_ago_hourly = warehouse.slice_metric(
            week_ago,
            previous_hour - dt.timedelta(days=7),
            PeriodType.HOUR,
            metric)
        week_ago_hourly = add_aggregations(week_ago_hourly)

        today_hourly = warehouse.slice_metric(
            start_of_today,
            previous_hour,
            PeriodType.HOUR,
            metric)
        today_hourly = add_aggregations(today_hourly)

        # filter out small agencies
        minimum_events = 2500 if cities_mode else 4000
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
        if not cities_mode:
            feeds = copy.copy(warehouse.get_feeds())
            feeds = feeds[feeds.country_codes != 'ZZ']
            feeds.country_codes.replace('EQ', 'EC', inplace=True)
            feeds = feeds.set_index('feed_code')[['feed_name', 'feed_location', 'country_codes', 'sub_country_codes']]
            feeds.loc[:, 'Country'] = feeds.country_codes.map(lambda code: pycountry.countries.get(alpha_2=code).name)
            feeds['State'] = None
            for i, row in feeds.iterrows():
                try:
                    codes = re.split(';|,', row.sub_country_codes.replace(' ',''))
                    states = [pycountry.subdivisions.get(code='{}-{}'.format(row.country_codes, sub_c)).name for sub_c in codes]
                    states = ', '.join(states)
                    feeds.at[i, 'State'] = states
                except Exception:
                    continue
            effect = pd.merge(feeds, effect, left_index=True, right_index=True, how='right')
            effect = effect.rename(columns={
                'feed_name': 'Name',
                'feed_location': 'Municipality',
            })
            # remove the state that sometimes comes after the city
            effect.Municipality = effect.Municipality.str.split(',').str[0]

            effect = effect.drop(columns=['country_codes', 'sub_country_codes'])

        effect = effect.replace([np.inf, -np.inf], np.nan)
        effect = effect.fillna('')

        # sort by municipality
        effect = effect.sort_values('Municipality')

        # export to google sheet
        gsheet = '1d3YKhnd1F0xg-S_FifIQbsrX-FoIs4Q94ALbnuSPZWw'
        staging = '1uaCfOpnX8s_Bf0LwIsVFUSBIWhQ34nGx41xcjyKYmdY'
        effect.columns = effect.columns.map(str)
        sheet = 'raw_cities' if cities_mode else 'raw'
        export_data_to_sheet(effect, None, gsheet, sheet=sheet)
