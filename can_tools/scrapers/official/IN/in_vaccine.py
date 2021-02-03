import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class IndianaVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://datavizpublic.in.gov/views/Dashboard/Vaccination"
    state_fips = int(us.states.lookup("Indiana").fips)
    location_type = "county"
    baseurl = "https://datavizpublic.in.gov"
    viewPath = "Dashboard/Vaccination"

    def fetch(self) -> pd.DataFrame:
        extra_get_query = {":isGuestRedirectFromVizportal": "y"}
        return self.get_tableau_view(extra_get_query=extra_get_query)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return data
