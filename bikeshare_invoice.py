from etl import sql_queries
from etl.date_utils import Period, PeriodType
from etl.google import export_data_to_sheet
from etl.google_sheet_getters import get_google_sheet
import datetime

spreadsheet_id = '12c9pADfn2EPKZhtmCP8L6Lc7yUGyB0ptzBcrHzA79V4'

any_date_last_quarter = datetime.datetime.today() - datetime.timedelta(days=90)
period = Period(any_date_last_quarter, PeriodType.QUARTER)

sql_con = sql_queries.Connector()
sales = sql_con.get_sales_data(period.start, period.end)

commission = get_google_sheet(spreadsheet_id, 'Commissions')
pass_type_dict = get_google_sheet(spreadsheet_id, 'Pass Type Dictionnary', range='A1:C')

export_data_to_sheet(sales, None, spreadsheet_id, 'data')
