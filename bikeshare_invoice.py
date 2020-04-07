from etl import sql_queries
from etl.date_utils import PeriodType
from etl.DataWarehouse import Period
from etl.google import export_data_to_sheet
from etl.google_sheet_getters import get_google_sheet
from etl.Metric import BikeshareAmplitudeSales, BikeshareAmplitudeTicketsSold, BikeshareSales
import datetime
import pandas as pd

spreadsheet_id = '12c9pADfn2EPKZhtmCP8L6Lc7yUGyB0ptzBcrHzA79V4'

any_date_last_quarter = datetime.datetime.today() - datetime.timedelta(days=90)
quarter = Period(any_date_last_quarter, PeriodType.QUARTER)
jan = Period(datetime.datetime(2020, 1, 1), PeriodType.MONTH)
feb = Period(datetime.datetime(2020, 2, 1), PeriodType.MONTH)
mar = Period(datetime.datetime(2020, 3, 1), PeriodType.MONTH)

sales_1 = BikeshareSales().get(jan, None).Revenue
sales_2 = BikeshareAmplitudeSales().get(feb, None)
sales_3 = BikeshareAmplitudeSales().get(mar, None)
sales = sales_1.add(sales_2, fill_value=0).add(sales_3, fill_value=0)
sales.name = 'sales'

passes_sold_1 = BikeshareSales().get(jan, None).Passes
passes_sold_2 = BikeshareAmplitudeTicketsSold().get(feb, None)
passes_sold_3 = BikeshareAmplitudeTicketsSold().get(mar, None)
passes_sold = passes_sold_1.add(passes_sold_2, fill_value=0).add(passes_sold_3, fill_value=0)
passes_sold.name = 'passes sold'

rep = pd.merge(sales, passes_sold, left_index=True, right_index=True)

commission = get_google_sheet(spreadsheet_id, 'Commissions').set_index('Service')
commission.Percentage = commission.Percentage.map(lambda p: float(p.strip('%'))/100)
commission['Flat fee'] = commission['Flat fee'].map(float)
pass_type_dict = get_google_sheet(spreadsheet_id, 'Pass Type Dictionnary', range='A1:C')

rep['commission'] = rep.sales * commission.Percentage + commission['Flat fee']
rep['commission'] = rep['commission'].clip(lower=0)
rep = rep.dropna()
rep.index.rename('Service', inplace=True)
rep = rep.sort_index()
export_data_to_sheet(rep, quarter, spreadsheet_id, 'Invoice')
