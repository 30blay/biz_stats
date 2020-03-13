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


def update_metrics():
    now = datetime.datetime.now()
    period = Period(now - datetime.timedelta(hours=1), PeriodType.HOUR)

    warehouse.load(
        [
            period,
        ], [
            AgencyTripPlans(),
            AgencyGoTrips(),
            AgencyDownloads(),
            AgencySessions(),
            AgencySales(),
            AgencyTicketsSold(),
        ])


while True:
    while datetime.datetime.now().minute != 1:  # Wait 1 second until the start of the next hour
        sleep(1)

    update_metrics()
