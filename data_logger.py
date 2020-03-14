from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
import datetime

engine = create_engine('sqlite:///warehouse.db')

warehouse = DataWarehouse(engine)
warehouse.create_all()
warehouse.add_entities()

measure_start = datetime.datetime(2020, 3, 15)
measure_stop = datetime.datetime(2020, 3, 17, hour=23, minute=59)

now = datetime.datetime.now()
start = min(now, measure_start)
stop = min(now, measure_stop)

warehouse.load_between(measure_start, stop, PeriodType.HOUR, [
        AgencyTripPlans(),
        AgencyGoTrips(),
        AgencyStartGo(),
        AgencyDownloads(),
        AgencyUniqueUsers(),
        AgencyUncorrectedSessions(),
    ])
