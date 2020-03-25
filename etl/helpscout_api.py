import json
import requests
from etl.transitapp_api import get_feeds
from functools import lru_cache

time_format = '%Y-%m-%dT%H:%M:%SZ'


@lru_cache()
def get_tags():
    tags = dict()
    url = 'https://api.helpscout.net/v2/tags'
    token = get_token()
    while True:
        response = requests.get(url=url, headers={'Authorization': 'Bearer {}'.format(token)})
        data = json.loads(response.text)
        for tag in data['_embedded']['tags']:
            tags[tag['name']] = tag['id']
        try:
            url = data['_links']['next']['href']
        except KeyError:
            break
    return tags


def get_token():
    # get access token
    client_id = 'JxWb109NPeerikytd9d5r7gGKTTcC8je'
    secret = '6ba1acX3UYe5QLLqJ5OIy1FZnYwQXV0b'
    response = requests.post(url='https://api.helpscout.net/v2/oauth2/token',
                             data={
                                 'grant_type': 'client_credentials',
                                 'client_id': client_id,
                                 'client_secret': secret,
                             })
    token = json.loads(response.text)['access_token']
    return token


def get_email_report(start, end, tag):
    tags = get_tags()
    tag_id = tags[tag]

    token = get_token()
    r = requests.get(url='https://api.helpscout.net/v2/reports/email',
                     headers={'Authorization': 'Bearer {}'.format(token)},
                     params={
                         'start': start.strftime(time_format),
                         'end': end.strftime(time_format),
                         'tags': str(tag_id),
                     })
    msgs = json.loads(r.text)
    return msgs


def get_support_emails_by_feed_code(start, end):
    tags = get_tags()
    # filter those related to an agency
    df = get_feeds()

    def get_messages_received(feed_code):
        try:
            email_report = get_email_report(start, end, feed_code.lower())
        except KeyError:
            return None
        return email_report['current']['volume']['messagesReceived']

    # get report
    df = df.assign(messages_received=df.feed_code.map(get_messages_received))
    df = df.set_index('feed_code')
    return df.messages_received
