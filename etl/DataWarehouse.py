from sqlalchemy import Column, Integer, Float, String, DateTime, Enum as SQLEnum, UniqueConstraint, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.hybrid import hybrid_property
import sshtunnel

import datetime
import os
import pandas as pd
from tqdm import tqdm
from etl.date_utils import PeriodType, last_day_of_month
from etl.transitapp_api import get_feeds, get_routes, get_feed_groups, get_sharing_systems
from etl.Metric import MetricType, RouteHits, AgencyRatio
from enum import Enum

Base = declarative_base()


class Entity(Base):
    __tablename__ = 'entities'
    entity_id = Column(Integer, primary_key=True)
    type = Column(String(64))
    feed_id = Column(Integer, unique=True)
    sharing_system_id = Column(Integer, unique=True)
    group_id = Column(Integer, unique=True)


class Period(Base):
    __tablename__ = 'periods'
    period_id = Column(Integer, primary_key=True, autoincrement=True)
    start = Column(DateTime)
    type = Column(SQLEnum(PeriodType))
    __table_args__ = (UniqueConstraint('start', 'type'),)

    def __init__(self, any_time, type_):
        self.type = type_
        if type_ == PeriodType.FIVEMIN:
            self.start = any_time - datetime.timedelta(minutes=any_time.minute % 5, seconds=any_time.second, microseconds=any_time.microsecond)

        if type_ == PeriodType.HOUR:
            self.start = any_time.replace(minute=0, second=0, microsecond=0)

        if type_ == PeriodType.DAY:
            self.start = any_time.replace(hour=0, minute=0, second=0, microsecond=0)

        if type_ == PeriodType.MONTH:
            self.start = any_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if type_ == PeriodType.YEAR:
            self.start = any_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

        if type_ == PeriodType.QUARTER:
            start_month = 3 * ((any_time.month-1) // 3) + 1
            self.start = any_time.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)

    @hybrid_property
    def end(self):
        if self.type == PeriodType.FIVEMIN:
            return self.start + datetime.timedelta(minutes=5)

        if self.type == PeriodType.HOUR:
            return self.start.replace(minute=59, second=59)

        if self.type == PeriodType.DAY:
            return self.start.replace(hour=23, minute=59, second=59)

        if self.type == PeriodType.MONTH:
            return last_day_of_month(self.start).replace(hour=23, minute=59, second=59)

        if self.type == PeriodType.YEAR:
            return last_day_of_month(self.start.replace(month=12)).replace(hour=23, minute=59, second=59)

        if self.type == PeriodType.QUARTER:
            start_month = self.start.month
            end_month = start_month + 2
            return last_day_of_month(self.start.replace(month=end_month)).replace(hour=23, minute=59, second=59)

    @hybrid_property
    def days(self):
        """ Get the number of days in the Period, including start and end"""
        delta = self.end - self.start
        return delta.days + 1

    def __str__(self):
        return "{} starting at {}".format(self.type.name, self.start)


class AgencyFact(Base):
    __tablename__ = 'fact_agencies'
    entity_id = Column(Integer, ForeignKey(Entity.entity_id), primary_key=True)
    period_id = Column(Integer, ForeignKey(Period.period_id), primary_key=True)
    metric = Column(String(64), primary_key=True)
    value = Column(Float)
    last_update = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


class RouteFact(Base):
    __tablename__ = 'fact_routes'
    global_route_id = Column(Integer, ForeignKey(Entity.entity_id), primary_key=True)
    feed_id = Column(Integer)
    period_id = Column(Integer, ForeignKey(Period.period_id), primary_key=True)
    metric = Column(String(64), primary_key=True)
    value = Column(Float)
    last_update = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


class WarehouseMode(Enum):
    PRODUCTION = 1
    DEVELOPMENT = 2


class DataWarehouse:
    def __init__(self, amplitude_stops_changing=datetime.timedelta(days=60), load_before_pull=False):
        """
        Args:
            amplitude_stops_changing: timedelta for which we will fetch the metric from Amplitude because of delayed reporting
            load_before_pull: functions that pull from the warehouse will try to load missing data.
        """
        # get the mode from environment variable, or default to production
        try:
            self.mode = WarehouseMode._member_map_[os.environ['WAREHOUSE_ENV'].upper()]
        except KeyError:
            self.mode = WarehouseMode.PRODUCTION

        self.engine = None
        self.create_engine()

        self.verbose = self.mode == WarehouseMode.DEVELOPMENT
        self.load_before_pull = load_before_pull
        self.declarative_base = Base
        self.connection = self.engine.connect()
        self.feeds = None
        self.sharing_systems = None
        self.routes = None
        self.feed_groups = None
        self.recency_limit = {PeriodType.HOUR: datetime.timedelta(minutes=45),
                              PeriodType.DAY: datetime.timedelta(hours=6),
                              PeriodType.MONTH: datetime.timedelta(days=3),
                              PeriodType.YEAR: datetime.timedelta(days=30),
                              }
        self.amplitude_stops_changing = amplitude_stops_changing
        self.session = Session(self.engine)

        self._create_all()
        self._add_entities()

    def get_feeds(self):
        if self.feeds is None:
            self.feeds = get_feeds()
        return self.feeds

    def get_sharing_systems(self):
        if self.sharing_systems is None:
            self.sharing_systems = get_sharing_systems()
        return self.sharing_systems

    def get_routes(self):
        if self.routes is None:
            self.routes = get_routes()
        return self.routes

    def get_feed_groups(self):
        if self.feed_groups is None:
            self.feed_groups = get_feed_groups()
        return self.feed_groups

    def _create_all(self):
        self.declarative_base.metadata.create_all(self.engine)

    def _add_entities(self):
        """
        Update the 'entities' table with all feeds, sharing systems and groups
        Returns: nothing

        """
        feeds = self.get_feeds()
        sharing_systems = self.get_sharing_systems()

        existing = pd.read_sql_table(Entity.__tablename__, self.connection)

        entities = []
        for _, feed in feeds.iterrows():
            if feed['feed_id'] not in existing.feed_id:
                entities.append(Entity(type='feed', feed_id=feed['feed_id']))
        for _, sharing_system in sharing_systems.iterrows():
            if sharing_system['system_id'] not in existing.sharing_system_id:
                entities.append(Entity(type='sharing_system', sharing_system_id=sharing_system['system_id']))

        for entity in entities:
            try:
                self.session.add(entity)
                self.session.commit()
            except IntegrityError:
                self.session.rollback()

    def _merge_period(self, period):
        """
        Take a Period object in detached state and reconciles it with an existing Period in the DB, or creates one
        """
        try:
            period = self.session.query(Period).filter(
                Period.start == period.start,
                Period.type == period.type).one()
        except NoResultFound:
            period = self.session.merge(period)
            self.session.add(period)
            self.session.commit()

        return period

    def _get_entity_id(self, feed_id):
        entity = self.session.query(Entity).filter(Entity.feed_id == feed_id).one()
        return entity.entity_id

    def load(self, period_list, metric_list):
        for metric in metric_list:
            if metric.entity_type not in (MetricType.AGENCY, MetricType.SHARING_SERVICE, MetricType.ROUTE):
                raise ValueError('Unsupported metric type {}'.format(metric.entity_type))

            groups = None
            if metric.entity_type == MetricType.AGENCY:
                #groups = self.get_feed_groups()
                pass

            if self.verbose:
                print(metric.name)
            for period in tqdm(period_list, disable=not self.verbose):
                if period.period_id is None:
                    period = self._merge_period(period)

                if not self._needs_to_recalculate(period, metric, groups):
                    continue

                df = metric.get(period, groups, False)
                df = df.dropna()

                if len(df) == 0:
                    continue

                make_facts = {
                    MetricType.AGENCY: self._make_agency_facts,
                    MetricType.SHARING_SERVICE: self._make_sharing_facts,
                    MetricType.ROUTE:  self._make_route_fact,
                }
                update_facts = make_facts[metric.entity_type](metric, period.period_id, df)

                if not self._exists(period, metric, None):
                    # this is an optimisation
                    self.session.add_all(update_facts)
                else:
                    # this is slower but always works
                    for fact in update_facts:
                        self.session.merge(fact)
                self.session.commit()

    def _make_agency_facts(self, metric, period_id, series):
        entities_df = pd.read_sql_table('entities', self.connection)
        feeds = self.get_feeds().set_index('feed_code')['feed_id']

        feed_id_map = entities_df.set_index('feed_id').entity_id
        feed_id_map = feed_id_map[~pd.isna(feed_id_map.index)]

        df = pd.DataFrame(series)
        df['feed_id'] = df.index.map(feeds)
        df['entity_id'] = df.feed_id.map(feed_id_map)
        df = df.dropna(subset=['entity_id'])

        update_facts = []
        for _, fact in df.iterrows():
            update_facts.append(AgencyFact(
                period_id=period_id,
                metric=metric.name,
                entity_id=int(fact['entity_id']),
                value=str(fact.iloc[0]),
            ))
        return update_facts

    def _make_sharing_facts(self, metric, period_id, df):
        entities_df = pd.read_sql_table('entities', self.connection)

        sharing_systems = get_sharing_systems().set_index('name').system_id
        feed_id_map = entities_df.set_index('sharing_system_id').entity_id
        feed_id_map = feed_id_map[~pd.isna(feed_id_map.index)]

        df['sharing_system_id'] = df.index.map(sharing_systems)
        df['entity_id'] = df.feed_id.map(feed_id_map)
        df = df.dropna(subset=['entity_id'])

        update_facts = []
        for _, fact in df.iterrows():
            update_facts.append(AgencyFact(
                period_id=period_id,
                metric=metric.name,
                entity_id=int(fact['entity_id']),
                value=str(fact.iloc[0]),
            ))
        return update_facts

    def _make_route_fact(self, metric, period_id, series):
        routes = self.get_routes()

        df = pd.DataFrame(series)
        df['feed_id'] = df.index.map(routes.feed_id)
        df = df.dropna(subset=['feed_id'])

        update_facts = []
        for global_route_id, fact in df.iterrows():
            update_facts.append(RouteFact(
                period_id=period_id,
                metric=metric.name,
                global_route_id=global_route_id,
                feed_id=fact.feed_id,
                value=str(fact.iloc[0]),
            ))
        return update_facts

    def get_periods_between(self, start, stop, period_type):
        """Get all possible periods, and create them if they don't exist"""
        periods = []
        period = Period(start, period_type)
        while period.start <= stop:
            local_period = self._merge_period(period)
            periods.append(local_period)
            period = Period(period.end + datetime.timedelta(minutes=1), period_type)
        return periods
                
    def load_between(self, start, stop, period_type, metrics):
        periods = self.get_periods_between(start, stop, period_type)
        self.load(periods, metrics)

    def slice_feed(self, feed_code, metrics, start, stop, period_type):
        feeds = self.get_feeds()
        feed_id = int(feeds[feeds.feed_code == feed_code].feed_id.values[0])
        metric_names = [metric.name for metric in metrics]
        entity_id = self._get_entity_id(feed_id)
        periods = self.get_periods_between(start, stop, period_type)

        if self.load_before_pull:
            self.load(periods, metrics)

        query = self.session.query(AgencyFact.metric, Period.start, AgencyFact.value, AgencyFact.last_update)\
            .outerjoin(Period, Period.period_id == AgencyFact.period_id)\
            .filter(AgencyFact.entity_id == entity_id, AgencyFact.metric.in_(metric_names))\
            .filter(Period.start.between(start, stop), Period.type == period_type)

        df = pd.DataFrame(query.all())
        df = self.correct_for_delayed_reporting(df)
        df = df.pivot(index='start', columns='metric', values='value')
        df.to_clipboard()
        print('Copied to clipboard')
        return df

    def slice_period(self, any_time, period_type, metrics):
        # nothing will happen if they are already there
        period = self._merge_period(Period(any_time=any_time, type_=period_type))

        if self.load_before_pull:
            self.load([period], metrics)

        all_metric_names = [metric.name for metric in metrics]
        stored_metric_names = [metric.name for metric in metrics if not isinstance(metric, AgencyRatio)]
        feeds = self.get_feeds().set_index('feed_id').feed_code

        query = self.session.query(Entity.feed_id, AgencyFact.metric, Period.start, AgencyFact.value, AgencyFact.last_update) \
            .outerjoin(Period, Period.period_id == AgencyFact.period_id) \
            .outerjoin(Entity, Entity.entity_id == AgencyFact.entity_id) \
            .filter(Period.period_id == period.period_id,
                    AgencyFact.metric.in_(stored_metric_names))

        df = pd.DataFrame(query.all())
        df = self.correct_for_delayed_reporting(df)

        if len(df) == 0:
            raise ValueError("No data found for metrics {} at period {}".format(all_metric_names, period))

        df = df.pivot(index='feed_id', columns='metric', values='value')
        df.index = df.index.map(feeds)

        # calculate ratios
        ratios = [metric for metric in metrics if isinstance(metric, AgencyRatio)]
        for ratio in ratios:
            df[ratio.name] = df[ratio.metric1.name] / df[ratio.metric2.name]

        # reorder columns as specified by metrics list
        df = df[all_metric_names]

        return df

    def slice_metric(self, start, end, period_type, metric):
        periods = self.get_periods_between(start, end, period_type)

        if metric.entity_type not in [MetricType.AGENCY, MetricType.ROUTE]:
            raise ValueError("Unsupported metric type {}".format(metric.entity_type.name))

        if self.load_before_pull:
            self.load(periods, [metric])

        getter = {MetricType.AGENCY: self.slice_metric_agencies,
                  MetricType.ROUTE: self.slice_metric_routes}[metric.entity_type]
        df = getter(start, end, period_type, metric)

        return df

    def slice_metric_agencies(self, start, end, period_type, metric):
        query = self.session.query(Entity.feed_id, AgencyFact.metric, Period.start, AgencyFact.value,
                                   AgencyFact.last_update) \
            .outerjoin(Period, Period.period_id == AgencyFact.period_id) \
            .outerjoin(Entity, Entity.entity_id == AgencyFact.entity_id) \
            .filter(Period.start.between(start, end), Period.type == period_type) \
            .filter(AgencyFact.metric == metric.name)
        df = pd.DataFrame(query.all())

        if len(df) == 0:
            raise ValueError("No data found for metric {} between {} and {}".format(metric, start, end))

        df = self.correct_for_delayed_reporting(df)

        df = df.pivot(index='feed_id', columns='start', values='value')
        feeds = self.get_feeds().set_index('feed_id').feed_code
        df.index = df.index.map(feeds)
        df.index.rename('feed_code', inplace=True)
        return df

    def slice_metric_routes(self, start, end, period_type, metric):
        query = self.session.query(RouteFact.global_route_id, RouteFact.metric, Period.start, RouteFact.value,
                                   RouteFact.last_update) \
            .outerjoin(Period, Period.period_id == RouteFact.period_id) \
            .filter(Period.start.between(start, end), Period.type == period_type) \
            .filter(RouteFact.metric == metric.name)
        df = pd.DataFrame(query.all())

        if len(df) == 0:
            raise ValueError("No data found for metric {} between {} and {}".format(metric, start, end))

        df = self.correct_for_delayed_reporting(df)

        df = df.pivot(index='global_route_id', columns='start', values='value')
        return df

    def get_top_routes_and_hits(self, any_date, period_type, this_period_last_year=False, n=3):
        if not period_type == PeriodType.MONTH:
            raise ValueError('MONTH periods only')

        any_date_last_year = any_date.replace(year=any_date.year-1)
        feeds = self.get_feeds()

        df = self.get_top_routes(any_date, period_type, n)
        index = df.index
        hits_this_year = self.get_route_hits(any_date, period_type)
        hits_last_year = self.get_route_hits(any_date_last_year, period_type)

        hits_this_year = hits_this_year[['global_route_id', 'hits']].rename(columns={'hits': 'hits this month'})
        hits_last_year = hits_last_year[['global_route_id', 'hits']].rename(columns={'hits': 'hits this month last year'})

        df = pd.merge(df, hits_this_year, on='global_route_id', how='left')
        if this_period_last_year:
            df = pd.merge(df, hits_last_year, on='global_route_id', how='left')

        df.index = index
        df = df.drop(columns='global_route_id')
        df = df.unstack()
        df.columns = df.columns.swaplevel(0, 1)
        df.sort_index(axis=1, level=0, sort_remaining=False, inplace=True)
        df.columns = df.columns.map('#{0[0]} {0[1]}'.format)
        df.index = df.index.map(feeds.set_index('feed_id').feed_code)
        return df

    def get_route_hits(self, any_date, period_type):
        """
        Get the hits for each route, for a given period
        Args:
            any_date: a date in the period for which we want to get top routes
            period_type: the type of period for which we want to get top routes

        Returns: a pandas DataFrame where every row is a route
        """
        period = self._merge_period(Period(any_date, period_type))
        metric = RouteHits()
        if self.load_before_pull:
            self.load([period], [metric])

        query = self.session.query(RouteFact.feed_id, RouteFact.global_route_id, RouteFact.value) \
            .outerjoin(Period, Period.period_id == RouteFact.period_id) \
            .filter(Period.period_id == period.period_id, RouteFact.metric == metric.name)
        df = pd.DataFrame(query.all())
        df = df.rename(columns={'value': 'hits'})

        return df

    def get_top_routes(self, any_date, period_type, n=3):
        """
        Get the top routes for each feed
        Args:
            any_date: any date of the period for which we want to get top routes
            period_type: the type of period (PeriodType enum)
            n: number of top routes to get

        Returns: a pandas DataFrame where every row is a feed, and columns are the top routes names and hits
        """
        routes = self.get_routes()

        df = self.get_route_hits(any_date, period_type)
        df = df.sort_values('hits', ascending=False)
        df = df.groupby('feed_id').head(n)
        df['route name'] = df.global_route_id.map(routes.route_short_name)
        df['Rank'] = df.groupby('feed_id').cumcount() + 1
        df = df.set_index(['feed_id', 'Rank'])
        df = df.drop(columns='hits')

        return df

    def get_top_routes_for_feed(self, feed_id, start, stop):
        periods = self.get_periods_between(start, stop, PeriodType.MONTH)
        df = pd.DataFrame()
        for period in periods:
            month_df = self.get_top_routes(period.start, period.type)
            month_df = month_df[month_df.index == feed_id]
            month_df.index = [period.start]
            df = df.append(month_df)

        return df

    def correct_for_delayed_reporting(self, df):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        correction_factor = pd.read_csv('{}/amplitude_delay.csv'.format(cur_dir))
        df['delay'] = df.last_update - df.start
        df.loc[df.delay < datetime.timedelta(), 'delay'] = datetime.timedelta()
        df.delay = df.delay.astype('timedelta64[h]')
        df = pd.merge(df, correction_factor, on=['delay', 'metric'], how='left')
        df.loc[:, 'factor'].fillna(1, inplace=True)
        df.value = df.value * df.factor

        return df.drop(columns=['last_update', 'factor'])

    def _needs_to_recalculate(self, period, metric, groups):
        if groups is not None:
            raise ValueError('groups not supported yet')

        fact_table = {MetricType.AGENCY: AgencyFact, MetricType.ROUTE: RouteFact}[metric.entity_type]
        query = self.session.query(Period.start, fact_table.last_update) \
            .outerjoin(Period, Period.period_id == fact_table.period_id) \
            .filter(fact_table.period_id == period.period_id, fact_table.metric == metric.name) \
            .order_by(fact_table.last_update.desc())
        most_recent = query.first()
        if most_recent is None:
            return True

        recency = datetime.datetime.now() - most_recent.last_update
        if recency < self.recency_limit[period.type]:
            return False

        delay = most_recent.last_update - most_recent.start
        return delay < self.amplitude_stops_changing

    def _exists(self, period, metric, groups):
        if groups is not None:
            raise ValueError('groups not supported yet')

        fact_table = {MetricType.AGENCY: AgencyFact, MetricType.ROUTE: RouteFact}[metric.entity_type]
        query = self.session.query(fact_table.period_id) \
            .filter(fact_table.period_id == period.period_id, fact_table.metric == metric.name)
        any_record = query.first()

        return any_record is not None

    def create_engine(self):
        if self.mode == WarehouseMode.PRODUCTION:
            cur_dir = os.path.dirname(os.path.abspath(__file__))
            self.engine = create_engine('sqlite:///{}/../warehouse.db'.format(cur_dir))

        if self.mode == WarehouseMode.DEVELOPMENT:
            pub_key_file = '~/.ssh/id_rsa'
            db_port = 4888
            server = sshtunnel.SSHTunnelForwarder(
                ('stats.transitapp.com', 22),
                ssh_username='deploy',
                remote_bind_address=('127.0.0.1', 3306),
                local_bind_address=('127.0.0.1', db_port),
                ssh_pkey=pub_key_file
            )

            # server.start()

            stats_connection_str = 'mysql+pymysql://root:E%Y+U3bbA9K[Yo.q@localhost:{}/transit_biz_stats'.format(db_port)

            self.engine = create_engine(stats_connection_str)
