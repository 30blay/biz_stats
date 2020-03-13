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

now = datetime.datetime.now()
period = Period(now - datetime.timedelta(hours=1), PeriodType.HOUR)

warehouse.load(
    [
        period,
    ], [
        AgencySessions(),
        AgencySales(),
        AgencyTicketsSold(),
])

#
# warehouse.load([Period(date, PeriodType.YEAR)], [
#     AgencyRidership(),
#     AgencyRevenue(),
# ])
#
# warehouse.load(
#     [
#         Period(date, PeriodType.MONTH),
#         Period(date.replace(year=date.year-1), PeriodType.MONTH),
#     ], [
#         AgencyDar(),
#         AgencyDau(),
#         AgencyMau(),
#         AgencySessions(),
#         AgencyAdoption(),
#         AgencySales(),
#         AgencyTicketsSold(),
#         AgencyAlertsSubs(),
#         SupportEmails(),
#         RouteHits(),
#         AgencyGoTrips(),
# ])
