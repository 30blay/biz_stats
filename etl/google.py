import datetime
import pickle
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


def create_service():
    """ create quickstart service and save credentials as a pickle file """
    CLIENT_SECRET_FILE = 'client_secret.json'
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    cred = None

    pickle_file = 'token_'+API_SERVICE_NAME+'_'+API_VERSION+'.pickle'

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        return service
    except Exception as e:
        print(e)
    return None


def export_data_to_sheet(df, date, spreadsheet_id, sheet='Sheet1', cell='A1'):
    """
    Export a pandas DataFrame to a google sheet.
    The index will be included as a column named index
    The sheet gets cleared first
    :param df: Pandas DataFrame object
    :param date: month the report is about, or None
    :param spreadsheet_id: The id of the spreadsheet to which to write. Can be found in it's URL
    :param sheet: The name of the sheet.
    :return: None
    """
    service = create_service()

    # clear everything first
    response = service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=sheet+'!A1:ZZZ',
    ).execute()

    if date:
        response = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            valueInputOption='RAW',
            range='{}!{}'.format(sheet, cell),
            body=dict(
                majorDimension='ROWS',
                values=[['Month of ' + date.strftime('%Y-%m')]])
        ).execute()

    response = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        valueInputOption='RAW',
        range='{}!{}'.format(sheet, cell),
        body=dict(
            majorDimension='ROWS',
            values=df.reset_index().T.reset_index().T.values.tolist())
    ).execute()

    response = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        valueInputOption='RAW',
        range='{}!{}'.format(sheet, cell),
        body=dict(
            majorDimension='ROWS',
            values=[[''],
                    ['Updated on ' + str(datetime.date.today())],
                    ['Note : This spreadsheet gets overwritten on a regular basis']
                    ])
    ).execute()
