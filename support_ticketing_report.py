from etl.helpscout_api import get_conversations
import datetime as dt
import pandas as pd
from html2text import html2text
from etl.google import export_data_to_sheet
from etl.google_sheet_getters import get_google_sheet
import re
from etl.transitapp_api import get_feeds

gsheet_id = '1GYBV5HYwu63E43hvp4OcBeu3uGcW6VuNbew5ClRshZE'
sheet = '20-Ticketing'
existing = get_google_sheet(gsheet_id, sheet)
existing_ids = [int(re.search('(?<=conversation\/).\d+', link).group(0)) for link in existing.Email]

tags = ['ticketing', 'mobile ticketing']
mailbox = 'Info @ Transit'
conversations = get_conversations(tags, mailbox, modified_since=dt.datetime(2020, 3, 1))

df = pd.DataFrame(conversations)
df = df[~df.id.isin(existing_ids)]
df = df.assign(date=pd.to_datetime(df.createdAt))
df = df.assign(date=df.date.dt.strftime('%Y-%m-%d'))
df = df.assign(link='https://secure.helpscout.net/conversation/' + df.id.map(str))

email_body = []
for embd in df._embedded:
    body = html2text(embd['threads'][-1]['body']).replace('\n', ' ')
    max_len = 500
    if len(body) > max_len:
        body = body[:max_len] + '...'
    email_body.append(body)
df = df.assign(email_body=email_body)

df = df.assign(version='', platform='', type='')
for i, row in df.iterrows():
    try:
        version = re.search('\d+\.\d+\.\d+', row.subject).group(0)
    except Exception:
        version = ''
    try:
        platform = re.search('Android|iOS', row.subject).group(0)
    except Exception:
        platform = ''
    df.at[i, 'version'] = version
    df.at[i, 'platform'] = platform

feeds = get_feeds()
agencies = []
for tags in df.tags:
    # get list of strings
    tags = [tag['tag'].upper() for tag in tags]
    feed_codes = [tag for tag in tags if tag in feeds.feed_code.values]
    if len(feed_codes) == 0:
        agencies.append(None)
    else:
        agencies.append(feed_codes[0])
df = df.assign(agency=agencies)


report = df[['date', 'platform', 'version', 'agency', 'link', 'type', 'email_body']].set_index('date')
export_data_to_sheet(report, None, gsheet_id, sheet, clear=False, bottom_warning=False, header=False)
