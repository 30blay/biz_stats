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

start = pd.datetime(2019, 1, 1)
stop = pd.datetime(2020, 2, 1)

metrics = [
    # AgencyUniqueUsers(),
    AgencyUncorrectedSessions(),
]

# warehouse.load_between(
#     pd.datetime(2019, 2, 25),
#     pd.datetime(2019, 5, 30),
#     PeriodType.DAY,
#     metrics)

warehouse.load_between(
    pd.datetime.now() - datetime.timedelta(days=8),
    pd.datetime.now(),
    PeriodType.DAY,
    metrics)

warehouse.load_between(
    pd.datetime.now() - datetime.timedelta(days=8),
    pd.datetime.now() - datetime.timedelta(days=6),
    PeriodType.HOUR,
    metrics)

warehouse.load_between(
    pd.datetime.now() - datetime.timedelta(days=1),
    pd.datetime.now(),
    PeriodType.HOUR,
    metrics)
