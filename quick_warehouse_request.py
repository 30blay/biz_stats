from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *

engine = create_engine('sqlite:///warehouse.db')

warehouse = DataWarehouse(engine)

start = pd.datetime(2018, 1, 1)
stop = pd.datetime(2019, 12, 1)

metrics = [
    AgencyMau(),
    AgencyDownloads(),
    AgencySessions(),
]

# warehouse.load_between(start, stop, PeriodType.MONTH, metrics)

# df = warehouse.slice_period(pd.datetime(2020, 1, 1), PeriodType.MONTH, metrics)
df = warehouse.slice_feed('STM', metrics, start, stop, PeriodType.MONTH)
