import inspect
import flask
from flask import request, jsonify
import connexion
from etl.DataWarehouse import DataWarehouse
from etl.Metric import *
import datetime as dt
from etl.date_utils import PeriodType

warehouse = DataWarehouse()

allowed_metrics = [
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

allowed_metrics_dict = dict(zip([str(m) for m in allowed_metrics], allowed_metrics))


def slice_feed(feed_code, metrics, period_type, start):
    start = dt.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
    stop = dt.datetime.now()

    metrics = [allowed_metrics_dict[m] for m in metrics]

    period_type = PeriodType.__getitem__(period_type.upper())

    df = warehouse.slice_feed(feed_code, metrics, start, stop, period_type)

    return df.to_json()


# If we're running in stand alone mode, run the application
if __name__ == '__main__':
    # Create the application instance
    app = connexion.App(__name__, specification_dir='./')

    # Read the swagger.yml file to configure the endpoints
    app.add_api('swagger.yml')

    app.run(debug=True)
