from etl import hubspot
from etl.Metric import *
from etl.DataWarehouse import DataWarehouse

warehouse = DataWarehouse(load_before_pull=True)

hubspot.update_companies(
    metrics=[
        AgencyMau(),
        AgencyDau(),
        AgencyDar(),
        AgencyAdoption(),
        AgencyRidership('annual_agency_ridership'),
        AgencyRevenue('annual_fare_revenue'),
    ], warehouse=warehouse)
