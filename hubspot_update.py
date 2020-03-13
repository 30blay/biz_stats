from etl import hubspot
from etl.Metric import *

hubspot.update_companies(
    metrics=[
        AgencyMau(),
        AgencyDau(),
        AgencyDar(),
        AgencyAdoption(),
        AgencyRidership('annual_agency_ridership'),
        AgencyRevenue('annual_fare_revenue'),
    ])
