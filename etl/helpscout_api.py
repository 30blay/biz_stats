import json

import requests

from etl.transitapp_api import get_feeds


def get_support_emails(start, end):
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

    # get tags
    tags = dict()
    url = 'https://api.helpscout.net/v2/tags'
    while True:
        response = requests.get(url=url, headers={'Authorization': 'Bearer {}'.format(token)})
        data = json.loads(response.text)
        for tag in data['_embedded']['tags']:
            tags[tag['name'].upper()] = tag['id']
        try:
            url = data['_links']['next']['href']
        except KeyError:
            break

    # filter those related to an agency
    df = get_feeds()
    df = df.assign(tag_id=df['feed_code'].map(tags))
    df = df.dropna(subset=['tag_id'])

    # get report
    time_format = '%Y-%m-%dT%H:%M:%SZ'

    def get_messages_received(feed_code):
        r = requests.get(url='https://api.helpscout.net/v2/reports/email',
                         headers={'Authorization': 'Bearer {}'.format(token)},
                         params={
                             'start': start.strftime(time_format),
                             'end': end.strftime(time_format),
                             'tags': str(int(df[df.feed_code == feed_code].tag_id)),
                         })
        msgs = json.loads(r.text)['current']['volume']['messagesReceived']
        return msgs

    df = df.assign(messages_received=df.feed_code.map(get_messages_received))
    df = df.set_index('feed_code')
    return df
