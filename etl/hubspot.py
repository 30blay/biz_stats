import pandas as pd
import re
import datetime
from hubspot3.companies import CompaniesClient
from etl.date_utils import last_month as get_last_month, PeriodType
from etl.DataWarehouse import Period
import numpy as np


API_KEY = '75b22d2d-ee62-4e5d-ac48-48d764273686'


def update_companies(metrics, warehouse):
    client = CompaniesClient(api_key=API_KEY)
    today = datetime.datetime.today()
    last_month = Period(get_last_month(today), PeriodType.MONTH)
    last_year = Period(today.replace(year=today.year-1), PeriodType.YEAR)

    companies = client.get_all(extra_properties=['feed_code'])
    df = pd.DataFrame(companies)
    df = df.rename(columns={'id': 'hubspot_id'})
    df = df.dropna(subset=['feed_code'])
    groups = df[df.feed_code.str.contains(',')]
    group_names = groups.feed_code.values
    group_defs = [re.findall(r"[\w']+", group) for group in group_names]
    groups = dict(zip(group_names, group_defs))

    for metric in metrics:
        # print('Getting {}'.format(metric.name))
        if PeriodType.MONTH in metric.supported_period_types:
            metric_df = warehouse.slice_period(last_month, [metric])
        else:
            metric_df = warehouse.slice_period(last_year, [metric])

        metric_df.columns = [metric.name]
        df = pd.merge(df, metric_df, left_on='feed_code', right_index=True, how='left')

    for _, company in df.iterrows():
        properties = []
        for metric in metrics:
            if np.isnan(company[metric.name]):
                continue
            properties.append({'name': metric.name, 'value': str(company[metric.name])})
        client.update(company['hubspot_id'], {'properties': properties})
