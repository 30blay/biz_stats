from etl.DataWarehouse import DataWarehouse, Period, PeriodType
from etl.date_utils import last_month
from etl.Metric import *
import datetime

warehouse = DataWarehouse('stats_mysql')

now = datetime.datetime.now()
period = Period(now - datetime.timedelta(hours=1), PeriodType.MONTH)

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
