from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
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
    AgencySales(),
    AgencyTicketsSold(),
]

warehouse = DataWarehouse()


def load_feed_slice(warehouse, feed_code, start, stop):
    df = warehouse.slice_feed(feed_code, metrics, start, stop, PeriodType.MONTH)
    return df


def load_metric_slice(warehouse, metric, start, stop):
    df = warehouse.slice_metric(start, stop, PeriodType.MONTH, metric)
    return df


def main():

    page = st.sidebar.radio("", ["Slice Feed", "Slice Metric"])

    if page == "Slice Feed":
        start = dt.datetime.combine(st.date_input('start', dt.date(2018, 1, 1)), dt.datetime.min.time())
        stop = dt.datetime.combine(st.date_input('start', dt.date.today()), dt.datetime.min.time())
        feed_code = st.selectbox("Feed", warehouse.get_feeds().feed_code)
        df = load_feed_slice(warehouse, feed_code, start, stop)
        y_axis = st.selectbox("Metric", df.columns)
        c = alt.Chart(df.reset_index()).mark_line().encode(x='start', y=y_axis, tooltip=['start', y_axis])
        st.altair_chart(c, use_container_width=True)
        st.table(df)

    if page == "Slice Metric":
        start = dt.datetime.combine(st.date_input('start', dt.date(2018, 1, 1)), dt.datetime.min.time())
        stop = dt.datetime.combine(st.date_input('start', dt.date.today()), dt.datetime.min.time())
        metric = st.selectbox("Metric", metrics)
        feeds = st.multiselect('Feed codes', warehouse.get_feeds().feed_code)
        with st.spinner('Wait for it...'):
            df = load_metric_slice(warehouse, metric, start, stop).T[feeds].unstack().rename(metric.name).reset_index()
        c = alt.Chart(df).mark_line().encode(x='start', y=metric.name, color='feed_code')
        st.altair_chart(c, use_container_width=True)


if __name__ == "__main__":
    main()
    warehouse.connection.close()
