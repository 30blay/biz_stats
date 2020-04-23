import requests
import datetime as dt
import pandas as pd

from etl.date_utils import PeriodType
from etl.Metric import *


class BizStatsClient:
    def __init__(self):
        self.url = 'http://0.0.0.0:5000/biz/'

    def slice_feed(self, feed_code, metrics, start=dt.datetime(2018, 1, 1), stop=dt.datetime.now(), period_type=PeriodType.MONTH):
        endpoint = self.url + 'slice_feed'
        response = requests.get(endpoint, params={
            'feed_code': feed_code,
            'metrics': ','.join([str(m) for m in metrics]),
            'start': start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            # 'stop': stop,
            'period_type': period_type.name.lower(),
        })
        df = pd.read_json(response.json())
        df.index.rename('date', inplace=True)
        return df
