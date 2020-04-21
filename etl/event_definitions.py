from ..etl.pyamplitude.pyamplitude.apiresources import Segment, Event


def bikeshare_city_mau():
    event = Event('Open App')
    event.add_groupby(groupby_type='user', groupby_value='gp:Services Available')
    return event


def bikeshare_users():
    event = Event('Open App')
    event.add_groupby(groupby_type='user', groupby_value='gp:Memberships')
    return event


def bikeshare_taps():
    event = Event('Tap Nearby Service')
    event.add_groupby(groupby_type='event', groupby_value='Service')
    return event


def bikeshare_trip_plans():
    event = Event('Tap Routing Suggestion')
    event.add_groupby(groupby_type='event', groupby_value='Service')
    return event


def bikeshare_unlocks():
    event = Event('ce:Get or Got Unlock Code')
    event.add_groupby(groupby_type='event', groupby_value='Service')
    return event


def agency_user():
    event = Event('Open App')
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_download():
    event = Event('New User')
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def feed_go_trip():
    event = Event('ce:Complete Go trip with Public Transit')
    event.add_groupby(groupby_type='event', groupby_value='Feed ID')
    return event


def feed_tap_nearby_service():
    event = Event('Tap Nearby Service')
    event.add_filter('event', 'Type', 'is', ['Transit'])
    event.add_groupby(groupby_type='event', groupby_value='Feed ID')
    return event


def service_sales():
    event = Event('Place Order')
    event.add_filter('event', 'State', 'is', ['Completed'])
    # The first groupby can be used for property sums. This is how amplitude works :)
    event.add_measured_property('event', 'Price')
    event.add_groupby(groupby_type='event', groupby_value='Service')
    return event


def service_purchased_prop_sales():
    """ This property (purchasedProps.Price) should almost never appear after 2020-04-01 """
    event = Event('Place Order')
    event.add_filter('event', 'State', 'is', ['Completed'])
    # The first groupby can be used for property sums. This is how amplitude works :)
    event.add_measured_property('event', 'purchasedProps.Price')
    event.add_groupby(groupby_type='event', groupby_value='Service')
    return event


def service_tickets_sold():
    event = Event('Place Order')
    event.add_filter('event', 'State', 'is', ['Completed'])
    # The first groupby can be used for property sums. This is how amplitude works :)
    event.add_measured_property('event', 'Quantity')
    event.add_groupby(groupby_type='event', groupby_value='Service')
    return event


def agency_total_trip_plans():
    event = Event('ce:Total Trip Plans')
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_shared_vehicule_taps():
    event = Event('ce:Shared Vehicle Taps')
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_launch_service_app():
    event = Event('ce:Launch Service App')
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_complete_ridehail_trip():
    event = Event('Completed')
    event.add_filter('event', 'Type', 'is', ['Rideshare', 'Ridesharing'])
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_shared_vehicle_unlocks():
    event = Event('ce:Shared Vehicle Unlocks')
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_transit_account_sessions():
    event = Event('Open App')
    event.add_filter('user', 'gp:Memberships', 'is', ['Transit account', 'St. Catharines Transit Commission'])
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event


def agency_covid_notif_tap():
    event = Event('Tap Push Notification')
    event.add_filter('event', 'Campaign ID', 'is not', ['(none)'])
    event.add_groupby(groupby_type='user', groupby_value='gp:Agencies')
    return event
