from functools import lru_cache
import pandas as pd
from etl.google import create_service
from etl.date_utils import PeriodType


def get_google_sheet(spreadsheet_id, sheet='Sheet1', range='A1:YY', header=True):
    """ Get a google sheet into a pandas DataFrame """
    service = create_service()
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet+'!'+range,
    ).execute()
    data = response['values']
    if header:
        df = pd.DataFrame(data[1:], columns=data[0][0:max([len(row) for row in data[1:]])])
    else:
        df = pd.DataFrame(data)
    return df


@lru_cache()
def get_dar(period):
    if period.type != PeriodType.MONTH:
        raise ValueError('get_dar only supports month periods')

    ridership_doc = '1HxFpjjaeeVRav4WITtKzSX2EnDJ7VbpqOu9gkiKMb8U'

    formatted_month = period.start.strftime("%Y-%m")

    df = get_google_sheet(ridership_doc, 'Monthly DAR')
    df = df.rename(columns={'index': 'feed_code', formatted_month: 'dar'})
    df = df[['feed_code', 'dar']]
    df = df.dropna()

    df = df.set_index('feed_code')
    df = pd.to_numeric(df.dar)
    return df


@lru_cache()
def get_yearly_agency_metric(period, sheet, metric_name):
    if period.type != PeriodType.YEAR:
        raise ValueError('year periods only')

    ridership_doc = '1HxFpjjaeeVRav4WITtKzSX2EnDJ7VbpqOu9gkiKMb8U'

    year = str(period.start.year)

    df = get_google_sheet(ridership_doc, sheet)
    df = df.rename(columns={'index': 'feed_code', year: metric_name})
    df = df[['feed_code', metric_name]]
    df = df.dropna()

    df = df.set_index('feed_code')
    df = pd.to_numeric(df[metric_name])
    return df


@lru_cache()
def get_unlinked_trips(period):
    return get_yearly_agency_metric(period, 'Yearly unlinked trips', 'unlinked trips')


@lru_cache()
def get_revenue(period):
    return get_yearly_agency_metric(period, 'Yearly revenue', 'revenue')
