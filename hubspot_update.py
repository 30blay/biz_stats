from etl import hubspot
from etl.Metric import *
from etl.DataWarehouse import DataWarehouse
import os
from sqlalchemy import create_engine

cur_dir = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///{}/warehouse.db'.format(cur_dir))
warehouse = DataWarehouse(engine)

hubspot.update_companies(
    metrics=[
        AgencyMau(),
        AgencyDau(),
        AgencyDar(),
        AgencyAdoption(),
        AgencyRidership('annual_agency_ridership'),
        AgencyRevenue('annual_fare_revenue'),
    ], warehouse=warehouse)
