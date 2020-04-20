from etl.DataWarehouse import DataWarehouse, Period, PeriodType
from etl.date_utils import last_month
from etl.Metric import *
import datetime as dt

warehouse = DataWarehouse('stats_mysql')

start = dt.datetime(2019, 1, 1)
stop = dt.datetime.now()

warehouse.load_between(
    start,
    stop,
    PeriodType.MONTH,
    [
        AgencyTripPlans(),
        AgencyGoTrips(),
        AgencyDownloads(),
        AgencySessions(),
        AgencySales(),
        AgencyTicketsSold(),
    ])
