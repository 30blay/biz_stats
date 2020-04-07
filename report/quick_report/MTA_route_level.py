from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
from etl.google import export_data_to_sheet
import datetime as dt
import copy
import os


cur_dir = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///{}/../../warehouse.db'.format(cur_dir))

warehouse = DataWarehouse(engine, amplitude_stops_changing=dt.timedelta(days=20))

metric = RouteHits()

staging = '1uaCfOpnX8s_Bf0LwIsVFUSBIWhQ34nGx41xcjyKYmdY'

feed_codes = ['MTAS', 'MTAMNT', 'MTABK', 'MTABX', 'MTAQN', 'MTASI', 'MTABC']

if __name__ == "__main__":

    benchmark19_slice = warehouse.slice_metric(
        pd.datetime(2019, 2, 15),
        pd.datetime(2019, 2, 28),
        PeriodType.DAY,
        metric)

    benchmark20_slice = warehouse.slice_metric(
        pd.datetime(2020, 2, 15),
        pd.datetime(2020, 2, 28),
        PeriodType.DAY,
        metric)

    mar19_slice = warehouse.slice_metric(
        pd.datetime(2019, 2, 8),
        pd.datetime(2019, 10, 30),
        PeriodType.DAY,
        metric)

    mar20_slice = warehouse.slice_metric(
        pd.datetime(2020, 2, 15),
        pd.datetime.now() - dt.timedelta(days=1),
        PeriodType.DAY,
        metric)

    routes = warehouse.get_routes()
    feeds = warehouse.get_feeds()
    feed_ids = feeds[feeds.feed_code.isin(feed_codes)].feed_id
    select_routes = routes[routes.feed_id.isin(feed_ids)]
    global_route_ids = select_routes.index

    benchmark19 = benchmark19_slice[benchmark19_slice.index.isin(global_route_ids)]

    benchmark20 = benchmark20_slice[benchmark20_slice.index.isin(global_route_ids)]

    mar19 = mar19_slice[mar19_slice.index.isin(global_route_ids)]

    mar20 = mar20_slice[mar20_slice.index.isin(global_route_ids)]

    # filter out small agencies
    minimum_events = 10
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
    effect = effect.dropna(axis=1, how='all').dropna()

    effect['short name'] = effect.index.map(select_routes.route_short_name)
    effect['long name'] = effect.index.map(select_routes.route_long_name)
    effect = effect.set_index('short name')

    # reverse, most recent on the left
    effect = effect.iloc[:, ::-1]

    effect = effect.replace([np.inf, -np.inf], np.nan)
    effect = effect.fillna('')

    # export to google sheet
    effect.columns = effect.columns.map(str)
    sheet = 'MTA routes'
    export_data_to_sheet(effect, None, staging, sheet=sheet)
