from sqlalchemy import MetaData, Column, Integer, Float, String, DateTime, Enum, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

import datetime
import pandas as pd
from tqdm import tqdm
from etl.date_utils import last_month, PeriodType
from etl import date_utils
from etl.transitapp_api import get_feeds, get_routes, get_feed_groups, get_sharing_systems
from etl.Metric import MetricType, RouteHits, AgencyRatio

meta = MetaData()
Base = declarative_base()


class Entity(Base):
    __tablename__ = 'entities'
    entity_id = Column(Integer, primary_key=True)
    type = Column(String)
    feed_id = Column(Integer, unique=True)
    sharing_system_id = Column(Integer, unique=True)
    group_id = Column(Integer, unique=True)


class Period(Base):
    __tablename__ = 'periods'
    period_id = Column(Integer, primary_key=True, autoincrement=True)
    start = Column(DateTime)
    type = Column(Enum(PeriodType))
    __table_args__ = (UniqueConstraint('start', 'type'),)


class AgencyFact(Base):
    __tablename__ = 'fact_agencies'
    entity_id = Column(Integer, ForeignKey(Entity.entity_id), primary_key=True)
    period_id = Column(Integer, ForeignKey(Period.period_id), primary_key=True)
    metric = Column(String, primary_key=True)
    value = Column(Float)
    last_update = Column(DateTime, default=datetime.datetime.now, primary_key=True)


class RouteFact(Base):
    __tablename__ = 'fact_routes'
    global_route_id = Column(Integer, ForeignKey(Entity.entity_id), primary_key=True)
    feed_id = Column(Integer)
    period_id = Column(Integer, ForeignKey(Period.period_id), primary_key=True)
    metric = Column(String, primary_key=True)
    value = Column(Float)
    last_update = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


class DataWarehouse:
    def __init__(self, engine):
        self.engine = engine
        self.declarative_base = Base
        self.connection = self.engine.connect()
        self.feeds = None
        self.sharing_systems = None
        self.routes = None
        self.feed_groups = None

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

    def create_all(self):
        self.declarative_base.metadata.create_all(self.engine)

    def add_periods(self, start=datetime.date(2015, 1, 1)):
        """
        Add record to the periods table until today. This does not calculate any metric for these periods
        If any of the periods already exists, that period is not added
        Args:
            start: datetime.date object

        Returns: nothing

        """
        session = Session(self.engine)
        today = datetime.date.today()
        periods = []
        first_day_of_month = last_month(today)

        while first_day_of_month > start:
            periods.append(Period(start=first_day_of_month, type=PeriodType.MONTH))
            periods.append(Period(start=first_day_of_month, type=PeriodType.YEAR))
            first_day_of_month = last_month(first_day_of_month)

        for period in periods:
            try:
                session.add(period)
                session.commit()
            except IntegrityError:
                session.rollback()
        session.close()

    def add_entities(self):
        """
        Update the 'entities' table with all feeds, sharing systems and groups
        Returns: nothing

        """
        feeds = self.get_feeds()
        sharing_systems = self.get_sharing_systems()

        entities = []
        for _, feed in feeds.iterrows():
            entities.append(Entity(type='feed', feed_id=feed['feed_id']))
        for _, sharing_system in sharing_systems.iterrows():
            entities.append(Entity(type='sharing_system', sharing_system_id=sharing_system['system_id']))

        session = Session(self.engine)
        for entity in entities:
            try:
                session.add(entity)
                session.commit()
            except IntegrityError:
                session.rollback()
        session.close()

    def _get_period_id(self, period):
        start = period.start
        type = period.type
        session = Session(self.engine)

        try:
            session.add(Period(start=period.start, type=period.type))
            session.commit()
        except IntegrityError:
            session.rollback()

        period = session.query(Period).filter(
                Period.start == start,
                Period.type == type).one()

        session.close()
        return period.period_id

    def _get_entity_id(self, feed_id):
        session = Session(self.engine)
        entity = session.query(Entity).filter(
            Entity.feed_id == feed_id).one()
        session.close()
        return entity.entity_id

    def load(self, period_list, metric_list):
        for metric in metric_list:
            if metric.entity_type not in (MetricType.AGENCY, MetricType.ROUTE):
                raise ValueError('Unsupported metric type {}'.format(metric.entity_type))

            groups = None
            if metric.entity_type == MetricType.AGENCY:
                #groups = self.get_feed_groups()
                pass

            print(metric.name)
            for period in tqdm(period_list):
                period_id = self._get_period_id(period)

                if period_id is None:
                    raise ValueError('Requested period is too recent')

                df = metric.get(period, groups, False)
                df = df.dropna()

                if len(df) == 0:
                    continue

                make_facts = {
                    MetricType.AGENCY: self._make_agency_facts,
                    MetricType.ROUTE:  self._make_route_fact,
                }
                update_facts = make_facts[metric.entity_type](metric, period_id, df)

                session = Session(self.engine)
                for fact in update_facts:
                    session.merge(fact)
                session.commit()
                session.close()

    def _make_agency_facts(self, metric, period_id, df):
        entities_df = pd.read_sql_table('entities', self.connection)
        feeds = self.get_feeds().set_index('feed_code')['feed_id']

        feed_id_map = entities_df.set_index('feed_id').entity_id
        feed_id_map = feed_id_map[~pd.isna(feed_id_map.index)]

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

    def _make_route_fact(self, metric, period_id, df):
        routes = self.get_routes()

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

    def periods_between(self, start, stop, period_type):
        periods = []
        period = date_utils.Period(start, period_type)
        while period.start <= stop:
            periods.append(period)
            period = date_utils.Period(period.end + datetime.timedelta(hours=1), period_type)
        return periods
                
    def load_between(self, start, stop, period_type, metrics):
        periods = self.periods_between(start, stop, period_type)
        self.load(periods, metrics)

    def get_feed_stats(self, feed_id, metrics, start, stop, period_type):
        metric_names = [metric.name for metric in metrics]
        entity_id = self._get_entity_id(feed_id)
        periods = self.periods_between(start, stop, period_type)
        period_ids = [self._get_period_id(period) for period in periods]

        session = Session(self.engine)
        query = session.query(AgencyFact.metric, Period.start, AgencyFact.value)\
            .outerjoin(Period, Period.period_id == AgencyFact.period_id)\
            .filter(Period.period_id.in_(period_ids), AgencyFact.entity_id == entity_id, AgencyFact.metric.in_(metric_names))

        df = pd.DataFrame(query.all())

        if len(df) == 0:
            raise ValueError("No data found for metrics {} at periods {}".format(metric_names, [str(p) for p in periods]))

        df = df.pivot(index='start', columns='metric', values='value')
        session.close()
        df.to_clipboard()
        print('Copied to clipboard')
        return df

    def get_period_stats(self, period, metrics):
        period_id = self._get_period_id(period)
        all_metric_names = [metric.name for metric in metrics]
        stored_metric_names = [metric.name for metric in metrics if not isinstance(metric, AgencyRatio)]
        feeds = self.get_feeds().set_index('feed_id').feed_code

        session = Session(self.engine)
        query = session.query(Entity.feed_id, AgencyFact.metric, Period.start, AgencyFact.value) \
            .outerjoin(Period, Period.period_id == AgencyFact.period_id) \
            .outerjoin(Entity, Entity.entity_id == AgencyFact.entity_id) \
            .filter(Period.period_id == period_id,
                    AgencyFact.metric.in_(stored_metric_names))

        df = pd.DataFrame(query.all())

        if len(df) == 0:
            raise ValueError("No data found for metrics {} at period {}".format(all_metric_names, period))

        df.value = pd.to_numeric(df.value)
        df = df.pivot(index='feed_id', columns='metric', values='value')
        df.index = df.index.map(feeds)

        # calculate ratios
        ratios = [metric for metric in metrics if isinstance(metric, AgencyRatio)]
        for ratio in ratios:
            df[ratio.name] = df[ratio.metric1.name] / df[ratio.metric2.name]

        # reorder columns as specified by metrics list
        df = df[all_metric_names]

        session.close()
        return df

    def get_top_routes_and_hits(self, period, this_period_last_year=False, n=3):
        if not period.type == PeriodType.MONTH:
            raise ValueError('MONTH periods only')

        period_last_year = Period(start=period.start.replace(year=period.start.year-1), type=period.type)
        feeds = self.get_feeds()

        df = self.get_top_routes(period, n)
        index = df.index
        hits_this_year = self.get_route_hits(period)
        hits_last_year = self.get_route_hits(period_last_year)

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

    def get_route_hits(self, period):
        """
        Get the hits for each route, for a given period
        Args:
            period: a Period object for which we want to get top routes

        Returns: a pandas DataFrame where every row is a route
        """
        period_id = self._get_period_id(period)
        metric = RouteHits()

        session = Session(self.engine)

        query = session.query(RouteFact.feed_id, RouteFact.global_route_id, RouteFact.value) \
            .outerjoin(Period, Period.period_id == RouteFact.period_id) \
            .filter(Period.period_id == period_id, RouteFact.metric == metric.name)
        df = pd.DataFrame(query.all())
        df = df.rename(columns={'value': 'hits'})

        return df

    def get_top_routes(self, period, n=3):
        """
        Get the top routes for each feed
        Args:
            period: a Period object for which we want to get top routes
            n: number of top routes to get

        Returns: a pandas DataFrame where every row is a feed, and columns are the top routes names and hits
        """
        routes = self.get_routes()

        df = self.get_route_hits(period)
        df = df.sort_values('hits', ascending=False)
        df = df.groupby('feed_id').head(n)
        df['route name'] = df.global_route_id.map(routes.route_short_name)
        df['Rank'] = df.groupby('feed_id').cumcount() + 1
        df = df.set_index(['feed_id', 'Rank'])
        df = df.drop(columns='hits')

        return df

    def get_top_routes_for_feed(self, feed_id, start, stop):
        periods = self.periods_between(start, stop, PeriodType.MONTH)
        df = pd.DataFrame()
        for period in periods:
            month_df = self.get_top_routes(period)
            month_df = month_df[month_df.index == feed_id]
            month_df.index = [period.start]
            df = df.append(month_df)

        return df
