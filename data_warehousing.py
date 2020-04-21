from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
import datetime as dt

warehouse = DataWarehouse()

start = dt.datetime(2018, 1, 1)
stop = dt.datetime.now()

warehouse.load_between(
    start,
    stop,
    PeriodType.MONTH,
    [
        AgencyMau(),
        AgencyDau(),
        AgencyTripPlans(),
        AgencyGoTrips(),
        AgencyDownloads(),
        AgencySessions(),
        AgencySales(),
        AgencyTicketsSold(),
    ])
