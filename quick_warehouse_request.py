from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.date_utils import Period, PeriodType, last_month
from etl.Metric import *
import datetime

engine = create_engine('sqlite:///warehouse.db')

warehouse = DataWarehouse(engine)
warehouse.create_all()
warehouse.add_periods()
warehouse.add_entities()

start = pd.datetime(2020, 3, 13)
stop = pd.datetime(2020, 3, 14)

metrics = [
    AgencyGoTrips(),
    AgencyDownloads(),
    AgencySessions(),
    AgencySales(),
    AgencyTicketsSold(),
]

# warehouse.load_between(start, stop, PeriodType.MONTH, metrics)

df = warehouse.get_feed_stats(1, metrics, start, stop, period_type=PeriodType.HOUR)
