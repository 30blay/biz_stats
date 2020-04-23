from BizStatsClient import BizStatsClient
from etl.Metric import *
from etl.transitapp_api import get_feeds
import streamlit as st
import altair as alt
import matplotlib.pyplot as plt
import datetime as dt
plt.style.use('transit')

metrics = [
    AgencyMau(),
    AgencyDau(),
    AgencyTripPlans(),
    AgencyGoTrips(),
    AgencyDownloads(),
    AgencySessions(),
    AgencyUncorrectedSessions(),
    AgencySales(),
    AgencyTicketsSold(),
    AgencyAlertsSubs(),
]

warehouse = BizStatsClient()


# @st.cache()
def load_feed_slice(feed_code, period_type, start, stop):
    df = warehouse.slice_feed(feed_code, metrics, start, stop, period_type=period_type)
    return df


def load_metric_slice(metric, start, stop, period_type):
    df = warehouse.slice_metric(start=start, stop=stop, period_type=period_type, metric=metric)
    return df


def main():

    page = st.sidebar.radio("", ["Slice Feed", "Slice Metric"])

    if page == "Slice Feed":
        start = dt.datetime.combine(st.date_input('start', dt.date(2020, 1, 1)), dt.datetime.min.time())
        stop = dt.datetime.combine(st.date_input('stop', dt.date.today()), dt.datetime.min.time())
        feed_code = st.selectbox("Feed", get_feeds().feed_code)
        period_type = st.selectbox("Period Type", list(PeriodType), 3)
        df = load_feed_slice(feed_code, period_type, start, stop)
        y_axis = st.selectbox("Metric", df.columns)
        c = alt.Chart(df.reset_index()).mark_line().encode(x='date', y=y_axis, tooltip=['date', y_axis])
        st.altair_chart(c, use_container_width=True)
        st.table(df)

    if page == "Slice Metric":
        start = dt.datetime.combine(st.date_input('start', dt.date(2018, 1, 1)), dt.datetime.min.time())
        stop = dt.datetime.combine(st.date_input('stop', dt.date.today()), dt.datetime.min.time())
        period_type = st.selectbox("Period Type", list(PeriodType), 3)
        metric = st.selectbox("Metric", metrics)
        feeds = st.multiselect('Feed codes', get_feeds().feed_code)
        with st.spinner('Wait for it...'):
            df = load_metric_slice(metric, start, stop, period_type).T[feeds].unstack().rename(metric.name).reset_index()
        c = alt.Chart(df).mark_line().encode(x='date', y=metric.name, color='entity')
        st.altair_chart(c, use_container_width=True)


if __name__ == "__main__":
    main()
