import argparse
import datetime
from report.recurrent_reports import bikeshare, agencies, kpi, latest_metrics, stored_agencies
from etl.date_utils import last_month as get_last_month

reports = {
    'bikeshare': bikeshare,
    'agencies': agencies,
    'kpi': kpi,
    'recent_metrics': latest_metrics,
    'stored_agencies': stored_agencies,
}
valid_reports_str = str.join(', ', reports.keys())

parser = argparse.ArgumentParser(description='This program generates generates reports for the business team')

parser.add_argument('report', action="store", default=False,
                    help='Which report to generate. Valid options are: ' + valid_reports_str)

last_month = get_last_month(datetime.datetime.today())
parser.add_argument('--date', type=str, default=last_month.strftime('%Y-%m'), metavar='YYYY-MM',
                    help='The month for which the report should be generated, in format YYYY-MM')

parser.add_argument('--gsheetid', type=str, metavar='value', required=True,
                    help='The google sheet id to which to output the report. Can be found in the sheets url')

args = parser.parse_args()
date = datetime.datetime.strptime(args.date, '%Y-%m')

if args.report in reports:
    print('Generating report: '+args.report+' for '+args.date)
    reports[args.report](date, args.gsheetid)
else:
    print('No report named '+args.report+'. Valid options are: '+valid_reports_str)
