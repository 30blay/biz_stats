import requests
import json
import pandas as pd
import datetime
from functools import lru_cache
from retry import retry


@lru_cache()
def get_overlapping_agencies(hardcoded=True):
    if not hardcoded:
        overlapping = set()
        rectangles = {}
        feeds = get_feeds()
        for _, feed in feeds.iterrows():
            rectangles[feed.feed_id] = feed.bounds

        for id0, bounds0 in rectangles.items():
            for id1, bounds1 in rectangles.items():
                if id1 == id0:
                    continue

                if bounds0['min_lat'] is None or bounds1['min_lat'] is None:
                    continue

                conditions = [
                    bounds0['min_lat'] < bounds1['max_lat'],
                    bounds0['min_lon'] < bounds1['max_lon'],
                    bounds1['min_lat'] < bounds0['max_lat'],
                    bounds1['min_lon'] < bounds0['max_lon'],
                ]
                if all(conditions):
                    overlap_h = min(bounds0['max_lon'], bounds1['max_lon']) - max(bounds0['min_lon'], bounds1['min_lon'])
                    overlap_w = min(bounds0['max_lat'], bounds1['max_lat']) - max(bounds0['min_lat'], bounds1['min_lat'])
                    area_overlap = overlap_h * overlap_w
                    area0 = (bounds0['max_lon'] - bounds0['min_lon']) * (bounds0['max_lat'] - bounds0['min_lat'])
                    area1 = (bounds1['max_lon'] - bounds1['min_lon']) * (bounds1['max_lat'] - bounds1['min_lat'])

                    thresh = 0.25
                    if (area_overlap / area0) > thresh:
                        print('{} and {} -> {}'.format(id0, id1, (area_overlap / area0)))
                        overlapping.add(id0)

        return overlapping

    overlapping = ['BBB',
                   'GGT',
                   'NICE',
                   'ROAMT',
                   'STLEQC',
                   'STOQC',
                   'STTR',
                   'TRT',
                   'SDT',
                   'PIRC',
                   'CTWA',
                   'AIRDAB',
                   'ART',
                   'BCTCA',
                   'CBCOH',
                   'MIWAY',
                   'GOTRANSIT',
                   'UPEON',
                   'YRT',
                   'ACT',
                   'NFTANY',
                   'KCM',
                   'TANKKY',
                   'BCRTA',
                   ]
    return overlapping


def get_non_overlapping_agencies():
    # todo: make this better
    non_overlapping_feed_codes = [
        'COTA2OH',
        'GDRTAOH',
        'HLF',
        'KTON',
        'NMTA',
        'RTCNV',
        'WPG',
        'TSL',
        'SKT',
        'MBTA',
    ]
    return non_overlapping_feed_codes


@lru_cache()
def get_feeds():
    response = requests.get(url='http://api.transitapp.com/v3/admin/feed_complete?all_feeds=1',
                            headers={'authorization': 'Basic dGhldHJhbnNpdGFwcDpUcmFuc2l0NGV2YQ==',
                                     'cache-control': 'no-cache',
                                     'postman-token': 'a155f066-51ef-888b-469f-1823266b100b'})
    data = json.loads(response.text)
    df = pd.DataFrame(data)

    # for duplicated feed_code, keep only in_beta != 3
    duplicated = df.feed_code.duplicated(keep=False)
    in_beta_3 = df.in_beta == 3
    df = df[~(duplicated & in_beta_3)]

    return df[['feed_id', 'feed_code', 'feed_name', 'feed_network_name', 'bounds', 'feed_location', 'country_codes', 'sub_country_codes']]


@lru_cache()
def get_feed_groups():
    response = requests.get(url='http://api.transitapp.com/v3/admin/feed_groups',
                            headers={'authorization': 'Basic dGhldHJhbnNpdGFwcDpUcmFuc2l0NGV2YQ==',
                                     'cache-control': 'no-cache',
                                     'postman-token': 'a155f066-51ef-888b-469f-1823266b100b'})
    data = json.loads(response.text)
    df = pd.DataFrame(data)
    return df


@lru_cache()
def get_sharing_systems():
    response = requests.get(url='https://api.transitapp.com/v3/sharing_system_feeds?all=1')
    data = json.loads(response.text)
    df = pd.DataFrame(data['system_feeds'])
    return df


@lru_cache()
def get_map_layer_id():
    df = get_sharing_systems()

    df = pd.concat([
        df[['id', 'name']].rename(columns={'id': 'map_layer_id'}),
        df[['system_id', 'name']].rename(columns={'system_id': 'map_layer_id'}),
    ])
    df = df.rename(columns={'name': 'service'})
    return df


@lru_cache()
def get_alerts():
    """ Get the number of users subscribed to service alerts for each feed and each month """
    response = requests.get(url='https://api-k8s.transitapp.com/v3/alerts/subscriptions/stats',
                            headers={'authorization': 'Basic ZjVhYzVkNDM5YTRiNjg5OWI5ZTFmYmVkZGUxNzg0NDQ6MGYzZDEyZDQ5OGI4ODJkZDBmNWNkOTVkMTc4N2EzNTE='})
    data = json.loads(response.text)
    df = pd.DataFrame(data['subscription_stats'])
    df = df.rename(columns={'category_id': 'feed_id'})

    # this is because the month from the query is the creation month, so the value refers to the previous month
    df.created_at = pd.to_datetime(df.created_at)
    df['report_date'] = df.created_at - datetime.timedelta(days=3)
    df['month'] = pd.DatetimeIndex(df.report_date).month
    df['year'] = pd.DatetimeIndex(df.report_date).year

    df.feed_id = pd.to_numeric(df.feed_id)
    feed_id_df = get_feeds()
    alerts = pd.merge(df, feed_id_df, on='feed_id', how='left')
    return alerts


def get_service_property_name(feed_codes):
    """
    Get the service property for amplitude event "tap nearby service"
    :param feed_codes:
    :return:
    """
    # todo: use pd.Series function
    names = get_feeds()
    services = []
    for feed_code in feed_codes:
        service = names[names.feed_code == feed_code].feed_network_name
        if len(service) > 1:
            raise ValueError('more than one feed_codes for the same feed_network_name {}'.format(service))
        if service.iloc[0] == '' or service.iloc[0] is None:
            service = names[names.feed_code == feed_code].feed_name
        services.append(service.iloc[0])
    return services


@lru_cache()
@retry(tries=2)
def get_routes():
    # feeds = get_feeds()
    # all_feed_ids = ','.join(feeds.feed_id.apply(str))
    # url = 'https://api.transitapp.com/v3/admin/routes?feed_ids={}'.format(all_feed_ids)
    url = 'https://transitupdate.transitapp.com/routes/get/allHistoricalRoutes'
    response = requests.get(url)
    route_dicts = json.loads(response.text).get('routes')
    routes = pd.DataFrame(route_dicts)
    routes = routes[['global_route_id', 'feed_id', 'route_short_name', 'route_long_name', 'network_names']]
    routes = routes.set_index('global_route_id')
    return routes

