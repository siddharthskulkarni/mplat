import numpy as np
import pandas as pd
from scipy.optimize import newton
from io import StringIO
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from mplat.data.sets import Dataset
from mplat.data.objects import DataSource, DataObjectHandler, CSVDataObject
from mplat.finance.bootstrap import bootstrap_spot_rates, compute_ytm_from_spot


class USTreasuryDailyParRatesDataSource(DataSource):
    """Concrete implementation of Dataset for U.S. Treasury Daily Yields."""
    def __init__(self):
        self.from_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{}/all?type=daily_treasury_yield_curve&field_tdr_date_value={}&page&_format=csv"
        self.type = "USTreasuryDailyYields"

    def fetch(self, date = "{}".format(dt.now().year)):
        df = pd.read_csv(self.from_url.format(date, date))
        return df


class USTreasuryDailyYieldsDataset(Dataset):
    """Concrete implementation of Dataset for U.S. Treasury Daily Yields."""
    def __init__(self, handler: DataObjectHandler, date = "{}".format(dt.now().year)):
        self.handler = handler
        self._raw_data = {}
        self.data = {}
        us_dpar = USTreasuryDailyParRatesDataSource().fetch(date)
        self._raw_data[date] = us_dpar
        self._clean()
        # CSVDataObject(
        #     name = "USTreasuryDailyParRates_{}".format(date),
        #     data = us_dpar,
        #     handler = self.handler
        # )
    
    def create(self, *args, **kwargs):
        pass
        
    def _clean(self, *args, **kwargs):
        for key, df in self._raw_data.items():
            # validate column names
            labels = df.columns.tolist()
            _labels = np.array(
                [
                    "Date",
                    "1 Mo",
                    "1.5 Month",
                    "2 Mo",
                    "3 Mo",
                    "4 Mo",
                    "6 Mo",
                    "1 Yr",
                    "2 Yr",
                    "3 Yr",
                    "5 Yr",
                    "7 Yr",
                    "10 Yr",
                    "20 Yr",
                    "30 Yr",
                ]
            )
            assert np.array_equal(labels, _labels), "Column names don't match: {}".format(
                _labels
            )

            # ensure 'Date' is datetime
            df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")

            for i in range(len(_labels)):
                if "Mo" in labels[i]:  # months
                    months = float(labels[i].split(" ")[0])
                    # calculate actual days ahead from first row date
                    delta_date = df["Date"].iloc[0] + relativedelta(months=int(months))
                    # add fractional months as days
                    fractional_month = months - int(months)
                    if fractional_month > 0:
                        delta_date += relativedelta(
                            days=int(fractional_month * 30)
                        )  # approx fractional month
                    days_ahead = (delta_date - df["Date"].iloc[0]).days
                    labels[i] = float(days_ahead)
                elif "Yr" in labels[i]:  # years
                    years = float(labels[i].split(" ")[0])
                    delta_date = df["Date"].iloc[0] + relativedelta(years=int(years))
                    fractional_year = years - int(years)
                    if fractional_year > 0:
                        delta_date += relativedelta(days=int(fractional_year * 365))  # approx
                    days_ahead = (delta_date - df["Date"].iloc[0]).days
                    labels[i] = float(days_ahead)
                else:
                    labels[i] = labels[i]

            df.columns = labels
            # convert percent to decimal
            df = df.apply(lambda x: x / 100 if pd.api.types.is_numeric_dtype(x) else x)

            self.data[key] = df

    def _preprocess(self, *args, **kwargs):
        """Convert par rates to yields using bootstrapping."""
        for key, df in self._raw_data.items():
            df = bootstrap_spot_rates(df)
            df = compute_ytm_from_spot(df)
            self.data[key] = df