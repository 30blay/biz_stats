from etl import sql_queries
from etl.date_utils import PeriodType
from etl.DataWarehouse import Period
from etl.google import export_data_to_sheet
from etl.google_sheet_getters import get_google_sheet
from etl.Metric import BikeshareAmplitudeSales, BikeshareAmplitudeTicketsSold
import datetime
import pandas as pd

spreadsheet_id = '12c9pADfn2EPKZhtmCP8L6Lc7yUGyB0ptzBcrHzA79V4'

any_date_last_quarter = datetime.datetime.today() - datetime.timedelta(days=90)
quarter = Period(any_date_last_quarter, PeriodType.QUARTER)

sales = BikeshareAmplitudeSales().get(quarter, None)
sales.columns = ['sales']
passes_sold = BikeshareAmplitudeTicketsSold().get(quarter, None)
passes_sold.columns = ['passes sold']
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
