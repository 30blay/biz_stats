import inspect
import flask
from flask import request, jsonify
import connexion
from etl.DataWarehouse import DataWarehouse, WarehouseMode
from etl.Metric import *
import datetime as dt
from etl.date_utils import PeriodType

warehouse = DataWarehouse()

date_format = "%Y-%m-%dT%H:%M:%SZ"

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


def basic_auth(username, password, required_scopes=None):
    if username == 'admin' and password == 'secret':
        return {'sub': 'admin'}

    # optional: raise exception for custom error response
    return None


def slice_feed(feed_code, metrics, period_type, start, stop):
    start = dt.datetime.strptime(start, date_format)
    stop = dt.datetime.strptime(stop, date_format)

    metrics = [allowed_metrics_dict[m] for m in metrics]

    period_type = PeriodType.__getitem__(period_type.upper())

    df = warehouse.slice_feed(feed_code, metrics, start, stop, period_type)

    return df.to_json()


def slice_metric(metric, period_type, start, stop):
    start = dt.datetime.strptime(start, date_format)
    stop = dt.datetime.strptime(stop, date_format)

    metric = allowed_metrics_dict[metric]

    period_type = PeriodType.__getitem__(period_type.upper())

    df = warehouse.slice_metric(start, stop, period_type, metric)

    return df.to_json()


# If we're running in stand alone mode, run the application
if __name__ == '__main__':
    # Create the application instance
    app = connexion.App(__name__, specification_dir='./', host='0.0.0.0', port=5000)

    # Read the swagger.yml file to configure the endpoints
    app.add_api('swagger.yml')
    debug = warehouse.mode == WarehouseMode.DEVELOPMENT
    app.run(debug=debug)
