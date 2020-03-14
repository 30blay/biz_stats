from sqlalchemy import create_engine
from etl.DataWarehouse import DataWarehouse
from etl.date_utils import Period, PeriodType, last_month
from etl.Metric import *
import datetime
from time import sleep


engine = create_engine('sqlite:///warehouse.db')

warehouse = DataWarehouse(engine)
warehouse.create_all()
warehouse.add_entities()

measure_start = datetime.datetime(2020, 3, 13)
measure_stop = datetime.datetime(2020, 3, 14, hour=23, minute=59)

while True:
    while datetime.datetime.now().minute != 0:  # Wait 1 second until the start of the next hour
        sleep(1)

    now = datetime.datetime.now()
    stop = min(now, measure_stop)

    warehouse.load_between(measure_start, stop, PeriodType.HOUR, [
            AgencyTripPlans(),
            AgencyGoTrips(),
            AgencyStartGo(),
            AgencyDownloads(),
            AgencySessions(),
            AgencySales(),
            AgencyTicketsSold(),
        ])
    sleep(60)
