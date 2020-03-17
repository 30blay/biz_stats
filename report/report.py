import pandas as pd
from etl.google import export_data_to_sheet


class Report:
    def __init__(self, date, google_sheet_id, groups_only=False, warehouse=None):
        """
        Instanciate a Report object
        :param date: any date in the month for which to produce the report
        :param google_sheet_id: which google sheet to output to. The Id is only part of the url
        :param groups_only: if True, include only groups of feeds
        """
        self.g_sheet_id = google_sheet_id
        self.this_month = date
        self.df = pd.DataFrame()
        self.groups = None
        self.groups_only = groups_only
        self.warehouse = warehouse

    def add(self, metrics, periods):
        """
        Add one or more metrics to the report, calculated for one or more periods
        :param metrics: a list of Metric objects
        :param periods: dictionary where the period name is the key and a Period object is the value
        :return:
        """
        if self.warehouse is not None:
            self._add_from_warehouse(metrics, periods)
        else:
            self._add_by_get(metrics, periods)

    def _add_from_warehouse(self, metrics, periods):
        for metric in metrics:
            for period_name, period in periods.items():
                data = self.warehouse.slice_period(period, [metric])
                data.columns = ['{} {}'.format(metric.name, period_name)]
                self.df = pd.merge(self.df, data, left_index=True, right_index=True, how='outer')

    def _add_by_get(self, metrics, periods):
        """
        Add one or more metrics to the report, calculated for one or more periods
        :param metrics: a list of Metric objects
        :param periods: dictionary where the period name is the key and a Period object is the value
        :return:
        """
        for metric in metrics:
            new_df = pd.DataFrame()
            print(metric.name)

            if isinstance(metric.name, str):
                metric.name = [metric.name]

            for period_name, period in periods.items():
                names = [name + ' ' + period_name for name in metric.name]
                all_data = metric.get(period, self.groups, self.groups_only)
                
                new_df = new_df.reindex(new_df.index.union(all_data.index))
                new_df[names] = all_data

                duplicated = new_df[new_df.index.duplicated()]
                if len(duplicated != 0):
                    raise ValueError('Duplicated index in {}'.format(metric.__class__.__name__))
            self._add_cols(metric, periods, new_df)

    def _add_cols(self, metric, periods, new_df):
        # reorder to have metrics, then periods
        for name in metric.name:
            for period_name, period in periods.items():
                col_name = name + ' ' + period_name
                if col_name in self.df.columns:
                    raise Exception('Duplicated column name {}'.format(col_name))
                self.df = self.df.reindex(self.df.index.union(new_df.index))
                self.df[col_name] = new_df[col_name]
                self.df[col_name] = self.df[col_name].fillna(metric.default_value)

    def set_code_groups(self, groups):
        """ Code groups are added to the report along with the single feed codes

        Args:
            groups: dict with keys being group names and values being lists of feed codes
        """
        self.groups = groups

    def export_data_to_sheet(self, sheet='Sheet1', cell='A1', metrics_as_rows=True):
        self.df['INDEX'] = self.df.index.str.upper()
        self.df = self.df.sort_values('INDEX')
        del self.df['INDEX']
        if metrics_as_rows:
            self.df = self.df.transpose()
        export_data_to_sheet(self.df, self.this_month, self.g_sheet_id, sheet=sheet, cell=cell)
