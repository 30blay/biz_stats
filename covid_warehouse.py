from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
import datetime
import os

cur_dir = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///{}/warehouse.db'.format(cur_dir))

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
#     pd.datetime(2019, 2, 1),
#     pd.datetime(2019, 2, 14),
#     PeriodType.DAY,
#     metrics)
#
# warehouse.load_between(
#     pd.datetime(2020, 2, 1),
#     pd.datetime(2020, 2, 14),
#     PeriodType.DAY,
#     metrics)
#
# warehouse.load_between(
#     pd.datetime(2019, 2, 7),
#     pd.datetime(2019, 5, 30),
#     PeriodType.DAY,
#     metrics)
#
warehouse.load_between(
    pd.datetime.now() - datetime.timedelta(days=4),
    pd.datetime.now(),
    PeriodType.DAY,
    metrics)

warehouse.load_between(
    (pd.datetime.now() - datetime.timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0),
    (pd.datetime.now() - datetime.timedelta(days=7)),
    PeriodType.HOUR,
    metrics)

warehouse.load_between(
    pd.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    pd.datetime.now(),
    PeriodType.HOUR,
    metrics)
