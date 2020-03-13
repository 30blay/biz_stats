import requests
import pandas as pd
import enum

from .date_utils import last_day_of_month, weekday_avg, last_month, PeriodType
from .pyamplitude.pyamplitude.apiresources import Segment, Event
from etl import event_definitions
from etl.pyamplitude.pyamplitude.projectshandler import ProjectsHandler
from etl.pyamplitude.pyamplitude.amplituderestapi import AmplitudeRestApi
from functools import lru_cache


URL_Transit = 'https://api.transitapp.com/v3/proxies/cached?timeout=21600&url=https://amplitude.com/'
URL_Amplitude = "https://amplitude.com/"
api_key = '3687b056476e15e4fe1b346e559a4169'
secret_key = '363da5e9de5af315a65b790283c93bc8'

# for pyamplitude
str_format = "%Y%m%d"


@lru_cache()
def chart(chart_id):
    """ Query an existing amplitude chart """
    response = requests.get(url=URL_Transit + 'api/3/chart/' + str(chart_id) + '/query', auth=(api_key, secret_key))
    # Sometimes I got 401 from transit proxy but amplitude url worked, so this is just for robustness
    if response.status_code == 401:
        response = requests.get(url=URL_Amplitude + 'api/3/chart/' + str(chart_id) + '/query', auth=(api_key, secret_key))

    if response.status_code == 429:
        print(response.reason)
        quit(1)

    data = response.json()
    return data


class EventMode(enum.Enum):
    uniques = 1
    totals = 2
    avg = 3
    pct_dau = 4
    sums = 5


class AggregationType(enum.Enum):
    NONE = 1
    WEEKDAY_AVG = 2


def get_event_on_period(event, period, mode, amplitude_connection, aggregation_type=AggregationType.NONE, segment=None):
    """ Get a pandas dataframe for a single event with groupby member.

        Args:
            event (required):	Event to retrieve data for.

            amplitude_connection (required):      AmplitudeRestApi object

            period:    Any date in the month of interest

            mode (optional):	EventMode enum.

            aggregation_type (optional):	AggregationType enum.

            segment (optional): pyamplitude Segment object
    """
    if not isinstance(aggregation_type, AggregationType):
        raise ValueError('Invalid aggregation_type')

    if not isinstance(mode, EventMode):
        raise ValueError('Invalid mode')

    if not isinstance(event, Event):
        raise ValueError('Only one event at a time is supported')

    start = period.start
    end = period.end

    interval = {PeriodType.FIVEMIN: '-300000',
                PeriodType.HOUR: '-3600000',
                PeriodType.DAY: '1',
                PeriodType.MONTH: '30',
                }[period.type]

    if aggregation_type == AggregationType.WEEKDAY_AVG:
        interval = "1"

    if event.event_type == 'New User':
        group_by, segment = groupby_and_segment_from_new_user_event(event)
        api_response = amplitude_connection.get_active_and_new_user_count(start.strftime(str_format), end.strftime(str_format),
                                                                          interval=interval,
                                                                          group_by=group_by,
                                                                          m='new',
                                                                          segment_definitions=[segment])

    else:
        segments = []
        if segment is not None:
            segments = [segment]
        api_response = amplitude_connection.get_events(start.strftime(str_format), end.strftime(str_format),
                                                       [event], mode.name, interval, segments)

    data = api_response['data']
    if isinstance(data['seriesLabels'][0], list):
        series_labels = [label[1] for label in data['seriesLabels']]
    else:
        series_labels = data['seriesLabels']

    diction = dict(zip(series_labels, data['series']), )
    df = pd.DataFrame.from_dict(
        diction,
        columns=data['xValues'],
        orient='index')

    df = df.fillna(0)

    if period.type in [PeriodType.FIVEMIN, PeriodType.HOUR]:
        return df[period.start.strftime("%Y-%m-%dT%H:%M:%S")]
    elif aggregation_type == AggregationType.WEEKDAY_AVG:
        return weekday_avg(df)
    return df.iloc[:, 0]


def groupby_and_segment_from_new_user_event(event):
    """ Amplitude Dashboard API requires group by and segments to be structured differently when the event New User """
    if event.event_type != 'New User':
        raise ValueError('"New User" events only')

    if len(event.get_groupby()) > 1:
        raise ValueError('Only one group_by is supported for "New User" event')

    segment = Segment()
    group_by = []

    if len(event.get_groupby()) == 1:
        segment.add_filter(prop=event.get_groupby()[0]['value'],
                           op='is not', values=[])
        group_by.append(event.get_groupby()[0]['value'])

    if len(event.get_filters()) == 1:
        filter = event.get_filters()[0]
        segment.add_filter(prop=filter['subprop_key'],
                           op=filter['subprop_op'], values=filter['subprop_value'])

    return group_by, segment


def get_most_popular_bikeshare_stations(date, mode, amplitude_connection, n):
    """ Get the most popular stations for each service, as defined by the stations which saw the most unlocks
    Args:
        date: Any date in the month of interest
        mode: Amplitude mode ('Totals' or 'Uniques')
        amplitude_connection: AmplitudeRestApi object
        n: Get n most popular stations
    """
    event = event_definitions.bikeshare_unlocks()
    event.add_groupby(groupby_type='event', groupby_value='Station Name')
    data = get_event_on_period(event, date, mode, amplitude_connection)
    df = pd.DataFrame({
        'Service': data.index.to_series().str.split(';').str[0],
        'Station': data.index.to_series().str.split(';').str[1],
        'Count': data
    })
    # This is done in python and not in the amplitude query to reduce cost by 50% ;)
    df = df[df['Station'] != ' (none)']
    df = df.sort_values('Count', ascending=False)
    df = df.groupby('Service').head(n)
    df['Rank'] = df.groupby('Service').cumcount() + 1
    df = df.set_index(['Service', 'Rank'])
    df = df.unstack()
    return df


def get_connection():
    """Get a pyamplitude connection to amplitude REST API"""
    project_handle = ProjectsHandler(project_name='transit', api_key=api_key, secret_key=secret_key)
    transit_api2 = URL_Transit + 'api/2/'
    # todo : try going through 'transit_api2'
    amplitude_connection = AmplitudeRestApi(project_handler=project_handle, show_logs=False, show_query_cost=False)
    return amplitude_connection


def get_retention(amplitude_connection, period, group):
    # query chart A
    chart_id_A = '5urz58r'
    chart_id_B = '4i8mmqx'

    dfA = chart(chart_id_A)
    dfB = chart(chart_id_B)

    return ret
