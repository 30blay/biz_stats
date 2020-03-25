import pandas as pd
import numpy as np
import enum
from etl.amplitude import get_event_on_period, get_most_popular_bikeshare_stations, get_connection, get_retention, \
    EventMode, AggregationType, Segment
from etl import sql_queries
from etl import event_definitions
from etl.date_utils import PeriodType
from etl.helpscout_api import get_support_emails_by_feed_code
from etl.transitapp_api import get_alerts, get_overlapping_agencies, get_feeds, get_non_overlapping_agencies, get_service_property_name
from etl.google_sheet_getters import get_dar, get_google_sheet, get_unlinked_trips, get_revenue


def correct_for_overlap(replacement_metric, target_metric, ratio):
    """
    For metrics that use the "Agencies" user property in amplitude, overlapping agencies need to be calculated
    differently. In their case, the metric is based on another (agency specific) metric.
    :param replacement_metric: The metric on which to base the target metric approximation. It's index must be the
    "Feed ID" event property in Amplitude
    :param target_metric: The metric which we are trying to approximate. It's index must be feed codes
    :param ratio: target_metric / replacement_metric
    :return: The corrected pandas Series
    """
    feeds = get_feeds().copy()  # copy to avoid changing the cached original
    feeds.feed_id = feeds.feed_id.map(str)
    overlapping_feed_codes = get_overlapping_agencies()
    replacement_metric.index = replacement_metric.index.map(feeds.set_index('feed_id').feed_code)

    target_metric[target_metric.index.isin(overlapping_feed_codes)] = \
        replacement_metric[replacement_metric.index.isin(overlapping_feed_codes)] * ratio
    return target_metric


class MetricType(enum.Enum):
    AGENCY = 1
    SHARING_SERVICE = 2
    ROUTE = 3


class Metric:
    def __init__(self, name, metric_type, default_value=''):
        self.name = name
        self.entity_type = metric_type
        self.default_value = default_value
        self.groups_enabled = True
        self.supported_period_types = [PeriodType.YEAR, PeriodType.MONTH]

        if not isinstance(metric_type, MetricType):
            raise ValueError('Invalid metric type')

    def get(self, period, groups, groups_only=False):
        """
        Get the values for that metric for a specific period.
        Args:
            period: Period object
            groups: Dict or None. Keys are group names, values are lists of feeds.
            groups_only: If False, single feeds data is returned as well as the groups

        Returns: pandas DataFrame

        """
        all_data = None

        if groups is None:
            groups = dict()

        if not self.groups_enabled:
            all_data = self._get(period)

        else:
            if not groups_only:
                all_data = self._get(period, None)

            for group_name, group_def in groups.items():
                if (all_data is not None) and group_name in all_data.index:
                    raise ValueError('group name {} conflicts with existing entity'.format(group_name))

                if not isinstance(group_def, list):
                    raise ValueError('group definition must be a list. Got {}'.format(group_def.__class__))

                group_data = self._get(period, group_def)
                if len(group_data) == 0:
                    continue
                group_data.index = [group_name]
                if all_data is not None:
                    all_data = all_data.append(group_data)
                else:
                    all_data = group_data

        all_data = all_data[~all_data.index.isna()]
        if isinstance(all_data, pd.Series):
            all_data = pd.DataFrame(all_data)
        return all_data

    def _get_event(self, getter_fn, group=None):
        """ Get an amplitude event, with a filter for a given group. If there is no group, no filter is applied and
        there will be a 'group by' instead.

        Args:
            getter_fn: function which should return the event with a 'group by' which can be 'Service', 'Feed ID' or 'Agencies'
            group: list of feed codes

        Returns:

        """
        event = getter_fn()
        if group is not None:
            if self.entity_type != MetricType.AGENCY:
                raise ValueError('Only agency groups are supported for now')
            filter_type = event.groupby[0]['value']
            event.groupby = event.groupby[0:-1]

            if filter_type == 'gp:Agencies':
                event.add_filter('user', 'gp:Agencies', 'is', group)
            elif filter_type == 'Service':
                group_services = get_service_property_name(group)
                event.add_filter('event', 'Service', 'is', group_services)
            elif filter_type == 'Feed ID':
                feeds = get_feeds()
                group_ids = []
                for code in group:
                    try:
                        group_ids.append(str(feeds[feeds.feed_code == code].feed_id.values[0]))
                    except IndexError:
                        raise ValueError('No feed id found for feed code {}'.format(code))
                event.add_filter('event', 'Feed ID', 'is', group_ids)
        return event


amplitude_con = get_connection()
sql_con = sql_queries.Connector()


class BikeshareMau(Metric):
    def __init__(self):
        super().__init__('MAU', MetricType.SHARING_SERVICE, 0)

    def _get(self, period, group):
        event = self._get_event(event_definitions.bikeshare_city_mau, group)
        return get_event_on_period(event, period, EventMode.uniques, amplitude_con, AggregationType.NONE)


class BikeshareUsers(Metric):
    def __init__(self):
        super().__init__('Bikeshare Users', MetricType.SHARING_SERVICE, 0)

    def _get(self, period, group):
        event = self._get_event(event_definitions.bikeshare_users, group)
        return get_event_on_period(event, period, EventMode.uniques, amplitude_con, AggregationType.NONE)


class BikeshareTaps(Metric):
    def __init__(self):
        super().__init__('Taps', MetricType.SHARING_SERVICE, 0)

    def _get(self, period, group):
        event = self._get_event(event_definitions.bikeshare_taps, group)
        return get_event_on_period(event, period, EventMode.totals, amplitude_con, AggregationType.NONE)


class BikeshareTripPlans(Metric):
    def __init__(self):
        super().__init__('Trip plans', MetricType.SHARING_SERVICE, 0)

    def _get(self, period, group):
        event = self._get_event(event_definitions.bikeshare_trip_plans, group)
        return get_event_on_period(event, period, EventMode.totals, amplitude_con, AggregationType.NONE)


class BikeshareUnlocksTotals(Metric):
    def __init__(self):
        super().__init__('Unlocks', MetricType.SHARING_SERVICE, 0)

    def _get(self, period, group):
        event = self._get_event(event_definitions.bikeshare_unlocks, group)
        return get_event_on_period(event, period, EventMode.totals, amplitude_con, AggregationType.NONE)


class BikeshareUnlocksUniques(Metric):
    def __init__(self):
        super().__init__('Users who unlocked', MetricType.SHARING_SERVICE, 0)

    def _get(self, period, group):
        event = self._get_event(event_definitions.bikeshare_unlocks, group)
        return get_event_on_period(event, period, EventMode.uniques, amplitude_con, AggregationType.NONE)


class BikeshareSales(Metric):
    def __init__(self):
        self.interest_vars = ['Passes', 'Revenue']
        super().__init__(self.interest_vars, MetricType.SHARING_SERVICE, 0)
        self.groups_enabled = False

    def _get(self, period):
        sales = sql_con.get_sales_data(period.start, period.end)
        return sales.groupby('service').sum()[self.interest_vars]


class BikeshareMostPopStations(Metric):
    def __init__(self, n=3):
        """
        Get the number of unlocks for most popular stations
        Args:
            n: Will return n top stations for each service. (n=3 will return top 3 for each service)
        """
        self.n = n
        names = []
        for i in range(1, n+1):
            names.append('#'+str(i)+' station name')
            names.append('#'+str(i)+' station unlocks')
        super().__init__(names, MetricType.SHARING_SERVICE, '')
        self.groups_enabled = False

    def _get(self, period):
        most_pop = get_most_popular_bikeshare_stations(period, EventMode.totals, amplitude_con, self.n)
        df = pd.DataFrame()
        for i in range(1, self.n+1):
            df['#'+str(i)+' station name'] = most_pop.Station[i]
            df['#'+str(i)+' station unlocks'] = most_pop.Count[i]
        return df


class AgencyRatio(Metric):
    def __init__(self, metric1, metric2, name=None):
        self.metric1 = metric1
        self.metric2 = metric2
        if name is None:
            name = '{}/{}'.format(metric1.name, metric2.name)
        super().__init__(name, MetricType.AGENCY, '')

    def _get(self, period, group):
        val1 = self.metric1._get(period, group)
        val2 = self.metric2._get(period, group)
        val2 = val2.replace(0, np.nan)
        if group is None:
            ratio = val1 / val2
            ratio = ratio.dropna()
        else:
            val1.index = val2.index
            ratio = val1 / val2
        return ratio


class AgencySessions(Metric):
    def __init__(self):
        super().__init__('Sessions', MetricType.AGENCY, '')

    def get_sessions_to_taps(self, period):
        sessions_event = event_definitions.agency_user()
        sessions_event.groupby.clear()

        taps_event = event_definitions.feed_tap_nearby_service()
        taps_event.groupby.clear()

        ses = get_event_on_period(sessions_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)
        taps = get_event_on_period(taps_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)
        ratio = float(ses/taps)
        return ratio

    def _get(self, period, group):
        sessions_event = self._get_event(event_definitions.agency_user, group)
        taps_event = self._get_event(event_definitions.feed_tap_nearby_service, group)

        ses = get_event_on_period(sessions_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)
        taps = get_event_on_period(taps_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)
        sessions_to_taps = self.get_sessions_to_taps(period)

        ses = correct_for_overlap(taps, ses, sessions_to_taps)
        ses = ses.drop(index='(none)', errors='ignore')
        return ses


class AgencyDailySessions(Metric):
    def __init__(self):
        super().__init__('Average Daily Sessions', MetricType.AGENCY, '')

    def _get(self, period, group):
        ses = AgencySessions()._get(period, group)
        day_ses = ses / period.days()
        return day_ses


class AgencyDownloads(Metric):
    def __init__(self):
        super().__init__('Downloads', MetricType.AGENCY, '')

    def _get(self, period, group):
        event = self._get_event(event_definitions.agency_download, group)
        return get_event_on_period(event, period, EventMode.uniques, amplitude_con, AggregationType.NONE)


class AgencyMau(Metric):
    def __init__(self, name='MAU'):
        self.aggregation_type = AggregationType.NONE
        super().__init__(name, MetricType.AGENCY, '')

    def _get(self, period, group):
        users_event = self._get_event(event_definitions.agency_user, group)
        group_users = get_event_on_period(users_event, period, EventMode.uniques, amplitude_con, self.aggregation_type)
        all_users = get_event_on_period(event_definitions.agency_user(), period, EventMode.uniques, amplitude_con, self.aggregation_type)

        # For overlapping agencies, MAU is based on "tap nearby service" and a conversion ratio
        taps_event = self._get_event(event_definitions.feed_tap_nearby_service, group)
        group_taps = get_event_on_period(taps_event, period, EventMode.uniques, amplitude_con, self.aggregation_type)
        all_taps = get_event_on_period(event_definitions.feed_tap_nearby_service(), period, EventMode.uniques, amplitude_con, self.aggregation_type)

        # get conversion ratio
        feeds = get_feeds()
        non_overlapping_feed_codes = get_non_overlapping_agencies()
        non_overlapping_feed_ids = feeds[feeds.feed_code.isin(non_overlapping_feed_codes)].feed_id.map(str).values

        users_to_taps = all_users[all_users.index.isin(non_overlapping_feed_codes)].sum() /\
                        all_taps[all_taps.index.isin(non_overlapping_feed_ids)].sum()

        users = correct_for_overlap(group_taps, group_users, users_to_taps)
        users = users.drop(index='(none)', errors='ignore')
        return users.round()
        
        
class AgencyUniqueUsers(Metric):
    def __init__(self, name='Unique Users'):
        self.aggregation_type = AggregationType.NONE
        super().__init__(name, MetricType.AGENCY, 0)

    def _get(self, period, group):
        users_event = self._get_event(event_definitions.agency_user, group)
        users = get_event_on_period(users_event, period, EventMode.uniques, amplitude_con, self.aggregation_type)
        users = users.drop(index='(none)', errors='ignore')
        return users.round()


class AgencyUncorrectedSessions(Metric):
    def __init__(self, name='Uncorrected Sessions'):
        self.aggregation_type = AggregationType.NONE
        super().__init__(name, MetricType.AGENCY, 0)

    def _get(self, period, group):
        session_event = self._get_event(event_definitions.agency_user, group)
        sessions = get_event_on_period(session_event, period, EventMode.totals, amplitude_con, self.aggregation_type)
        sessions = sessions.drop(index='(none)', errors='ignore')
        return sessions.round()


class AgencyDau(AgencyMau):
    def __init__(self):
        super().__init__('DAU')
        self.aggregation_type = AggregationType.WEEKDAY_AVG


class AgencyGoTrips(Metric):
    def __init__(self):
        super().__init__('GO Trips', MetricType.AGENCY, '')

    def _get(self, period, group):
        go_event = self._get_event(event_definitions.feed_go_trip, group)
        df = get_event_on_period(go_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)
        feed_codes = get_feeds()[['feed_id', 'feed_code']].set_index('feed_id')
        feed_codes.index = feed_codes.index.astype(str)
        df.index = df.index.map(feed_codes.feed_code)
        return df


class AgencyAlertsSubs(Metric):
    def __init__(self):
        super().__init__('Service Alert Subscribers', MetricType.AGENCY, '')

    def _get(self, period, group):
        if period.type != PeriodType.MONTH:
            raise ValueError('AgencyAlerts only supports month periods')
        alerts = get_alerts()
        alerts = alerts.pivot_table(index='feed_code', columns=['year', 'month'], values='subscriber_count')
        try:
            alerts = alerts[period.start.year][period.start.month]
        except KeyError:
            alerts = pd.Series()
        if group:
            alerts = pd.Series(alerts[alerts.index.isin(group)].sum())
        return alerts


class AgencyMostPopLines(Metric):
    def __init__(self, n=3, same_month_last_year=True):
        self.n = n
        self.same_month_last_year = same_month_last_year
        names = []
        for i in range(1, n+1):
            names.append('#'+str(i)+' line name')
            names.append('#'+str(i)+' line hits this month')
            if self.same_month_last_year:
                names.append('#'+str(i)+' line hits this month last year')
        super().__init__(names, MetricType.AGENCY, '')

    def _get(self, period, group):
        most_pop = sql_con.get_most_popular_routes(period.start, self.n, group)
        ret = pd.DataFrame()
        for i in range(1, self.n+1):
            ret['#'+str(i)+' line name'] = most_pop['#' + str(i) + ' route_name']
            ret['#'+str(i)+' line hits this month'] = most_pop['#' + str(i) + ' hits']
            if self.same_month_last_year:
                ret['#'+str(i)+' line hits this month last year'] = most_pop['#' + str(i) + ' hits_this_month_last_year']

        return ret


class SupportEmails(Metric):
    def __init__(self):
        super().__init__('Support Emails', MetricType.AGENCY, 0)

    def _get(self, period, group):
        data = get_support_emails_by_feed_code(period.start, period.end)
        if group:
            data = pd.Series(data[data.index.isin(group)].sum())
        return data


class AgencyDar(Metric):
    def __init__(self):
        super().__init__('DAR', MetricType.AGENCY, '')

    def _get(self, period, group):
        data = get_dar(period)
        if group:
            data = pd.Series(data[data.index.isin(group)].sum())
        return data.round()


class AgencyRidership(Metric):
    def __init__(self, name='Yearly unlinked trips'):
        super().__init__(name, MetricType.AGENCY, '')
        self.supported_period_types = [PeriodType.YEAR]

    def _get(self, period, group):
        data = get_unlinked_trips(period)
        if group:
            data = pd.Series(data[data.index.isin(group)].sum())
        return data


class AgencyRevenue(Metric):
    def __init__(self, name='Yearly Fare Revenue'):
        super().__init__(name, MetricType.AGENCY, '')
        self.supported_period_types = [PeriodType.YEAR]

    def _get(self, period, group):
        data = get_revenue(period)
        if group:
            data = pd.Series(data[data.index.isin(group)].sum())
        return data


class AgencyAdoption(Metric):
    def __init__(self):
        super().__init__('Adoption', MetricType.AGENCY, '')
        self.metric = AgencyRatio(AgencyDau(), AgencyDar())

    def _get(self, period, group):
        return self.metric._get(period, group).round(4)


class AgencyRetention(Metric):
    def __init__(self):
        super().__init__('Retention, 1 month', MetricType.AGENCY, '')

    def _get(self, period, group):
        ret = get_retention(amplitude_con, period, group)
        return ret


class AgencyLanguage(Metric):
    def __init__(self):
        super().__init__('Language', MetricType.AGENCY, '')
        self.groups_enabled = False

    def _get(self, period):
            df = get_google_sheet('14TOJVCwU78wfSv8METHz-vpbbXmfkkEBwiblhE6-IC8', 'Recipients')
            df = df[['Agency', 'Language']].set_index('Agency')
            return df


class AgencyName(Metric):
    def __init__(self):
        super().__init__('Name', MetricType.AGENCY, '')
        self.groups_enabled = False

    def _get(self, period):
        df = get_feeds().set_index('feed_code')
        return df.feed_name


class AgencyLocation(Metric):
    def __init__(self, name='Location'):
        super().__init__(name, MetricType.AGENCY, '')
        self.groups_enabled = False

    def _get(self, period):
        df = get_feeds().set_index('feed_code')
        return df.feed_location


class AgencyCountry(Metric):
    def __init__(self):
        super().__init__('Country code', MetricType.AGENCY, '')
        self.groups_enabled = False

    def _get(self, period):
        df = get_feeds().set_index('feed_code')
        return df.country_codes


supported_sales_feed_codes = {'RTC': 'RTCNV',
                              'RTD': 'RTD',
                              'Big Blue Bus': 'BBB',
                              'St. Catharines Transit Commission': 'SCTCON',
                              }


class AgencySales(Metric):
    def __init__(self):
        super().__init__('Sales', MetricType.AGENCY, '-')

    def _get(self, period, group):
        sales_event = self._get_event(event_definitions.service_sales, group)
        data = get_event_on_period(sales_event, period, EventMode.sums, amplitude_con)
        data = data[data.index.isin(supported_sales_feed_codes.keys())]
        data.index = data.index.map(supported_sales_feed_codes)
        return data


class AgencyTicketsSold(Metric):
    def __init__(self):
        super().__init__('Tickets Sold', MetricType.AGENCY, '0')

    def _get(self, period, group):
        sales_event = self._get_event(event_definitions.agency_tickets_sold, group)
        data = get_event_on_period(sales_event, period, EventMode.sums, amplitude_con)
        data = data[data.index.isin(supported_sales_feed_codes.keys())]
        data.index = data.index.map(supported_sales_feed_codes)
        return data


class AgencyTripPlans(Metric):
    def __init__(self):
        super().__init__('Trip plans', MetricType.AGENCY, '')

    def _get(self, period, group):
        event = self._get_event(event_definitions.agency_total_trip_plans, group)
        return get_event_on_period(event, period, EventMode.totals, amplitude_con, AggregationType.NONE)


class AgencySharedVehicleTaps(Metric):
    def __init__(self):
        super().__init__('Shared vehicle taps', MetricType.AGENCY, '0')

    def _get(self, period, group):
        event = self._get_event(event_definitions.agency_shared_vehicule_taps, group)
        return get_event_on_period(event, period, EventMode.totals, amplitude_con, AggregationType.NONE)


class AgencyRidehailRequestsPlusTrips(Metric):
    def __init__(self):
        super().__init__('Ridehail requests', MetricType.AGENCY, '0')

    def _get(self, period, group):
        launch_event = self._get_event(event_definitions.agency_launch_service_app, group)
        launch_count = get_event_on_period(launch_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)

        trip_event = self._get_event(event_definitions.agency_complete_ridehail_trip, group)
        trip_count = get_event_on_period(trip_event, period, EventMode.totals, amplitude_con, AggregationType.NONE)
        return launch_count + trip_count


class AgencySharedVehicleUnlocks(Metric):
    def __init__(self):
        super().__init__('Shared vehicle unlocks', MetricType.AGENCY, '0')

    def _get(self, period, group):
        event = self._get_event(event_definitions.agency_shared_vehicle_unlocks, group)
        return get_event_on_period(event, period, EventMode.totals, amplitude_con, AggregationType.NONE)


class AgencyTransitAccounts(Metric):
    def __init__(self):
        super().__init__('Transit accounts', MetricType.AGENCY, '0')

    def _get(self, period, group):
        event = self._get_event(event_definitions.agency_transit_account_sessions, group)
        return get_event_on_period(event, period, EventMode.uniques, amplitude_con, AggregationType.NONE)


class AgencyCreditCardAccounts(Metric):
    def __init__(self):
        super().__init__('Credit Card Accounts', MetricType.AGENCY, '0')

    def _get(self, period, group):
        event = self._get_event(event_definitions.agency_transit_account_sessions, group)
        credit_card_accounts = Segment()
        credit_card_accounts.add_filter('userdata_cohort', 'is', ['o8jdi7p'])
        return get_event_on_period(event, period, EventMode.uniques, amplitude_con, AggregationType.NONE,
                                   segment=credit_card_accounts)


class RouteHits(Metric):
    def __init__(self):
        super().__init__('Hits', MetricType.ROUTE, '0')
        self.groups_enabled = False

    def _get(self, period):
        df = sql_con.get_route_hits(period.start, period.end)
        df = df.set_index('global_route_id').hits
        return df
