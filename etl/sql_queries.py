from sqlalchemy import create_engine
import pandas as pd
from .date_utils import last_day_of_month
from .transitapp_api import get_map_layer_id, get_feeds, get_routes


stats_connection_str = 'mysql+pymysql://readonly:EQgK}cNxev$i6mr@localhost:3306/transit_stats_production'
map_layer_ids = "(1, 9, 11, 15, 36, 537, 541, 550, 551, 561, 578, 589, 590, 649, 1012, 552, 4, 1007, 464, 560, 12)"


class Connector:
    def __init__(self):
        self.stats = create_engine(stats_connection_str)
        self.feed_to_name_map = None

    def get_sales_data(self, first_day, last_day):
        """ Query the stats database for purchases, and db1 to get a mapping from map_layer_id to service name """
        start = first_day.strftime('%Y-%m-%d %H:%M:%S')
        end = last_day.strftime('%Y-%m-%d %H:%M:%S')
        sales_sql = '''
        SELECT map_layer_id, item_name, SUM(item_count) AS Passes, SUM(total) AS Revenue
        FROM transit_stats_production.purchase
        WHERE item_name IS NOT NULL
        AND item_name <> ''
        AND item_name NOT LIKE "%%adeau%%"
        AND map_layer_id IN ''' + map_layer_ids + '''
        AND timestamp BETWEEN \'''' + start + '''\' AND \'''' + end + '''\'
        GROUP BY map_layer_id, item_name;
        '''
        sales = pd.read_sql(sales_sql, con=self.stats)

        map_layer_id_df = get_map_layer_id().drop_duplicates()
        sales = pd.merge(sales, map_layer_id_df, on='map_layer_id', how='left')

        return sales

    def get_route_hits(self, first_day, last_day):
        sql = '''SELECT feed_id, global_route_id, SUM(hits) as hits
        FROM global_route_report
        WHERE date BETWEEN \'''' + first_day.strftime('%Y-%m-%d') + '''\' AND \'''' + last_day.strftime('%Y-%m-%d') + '''\'
        AND hits >= 1
        AND feed_id != -1
        GROUP BY feed_id, global_route_id
        '''

        return pd.read_sql(sql, con=self.stats)

    def get_most_popular_routes(self, date, n=3, group=None):
        """
        Will get the most popular route on a given month

        Args:
            date: any day of that month
            n: return top n routes
            group: feed_code list

        """
        date_last_year = date.replace(year=date.year-1)

        df = self.get_route_hits(date.replace(day=1), last_day_of_month(date))
        df_last_year = self.get_route_hits(date_last_year.replace(day=1), last_day_of_month(date_last_year))
        df_last_year = df_last_year.rename(columns={'hits': 'hits_this_month_last_year'})
        feed_id_df = get_feeds()[['feed_id', 'feed_code']]

        df = pd.merge(df, df_last_year, on=['feed_id', 'global_route_id'], how='left')
        df = pd.merge(df, feed_id_df, on='feed_id', how='left')
        df = df.drop(columns='feed_id')

        if group:
            df = df[df.feed_code.isin(group)]
            df.feed_code = 'group'

        df = df.sort_values('hits', ascending=False)
        df = df.groupby('feed_code').head(n)

        # replace global route id with the route name
        routes = get_routes()
        df['route_name'] = df.global_route_id.map(routes.route_short_name)
        df = df.drop(columns='global_route_id')

        df['Rank'] = df.groupby('feed_code').cumcount() + 1
        df = df.set_index(['feed_code', 'Rank'])
        df = df.unstack()
        df.columns = df.columns.map('#{0[1]} {0[0]}'.format)

        return df
