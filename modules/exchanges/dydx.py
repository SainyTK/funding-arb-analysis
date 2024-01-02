import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import json
from glob import glob

class DYDXFetcher:
    funding_interval = 8
    markets_base = {}

    # Public functions
    def list_markets(self):
        if len(self.markets_base) == 0:
            self._init_markets()
        return list(self.markets_base.keys())

    def get_market_base(self, market):
        if len(self.markets_base) == 0:
            self._init_markets()
        return self.markets_base[market]

    def fetch_24h_vol(self, market):
        raw = self._fetch_24h_vol(market)
        return {
            "exchange": "dydx",
            "market": market,
            "timestamp": int(datetime.now().timestamp() * 1000),
            "volume": float(raw["baseVolume"]) if raw else 0,
        }

    def fetch_annualized_average_funding_rate(self, market):
        df = self.fetch_funding_rate_history_until_start(market)

        timeframes_preset = {
            "1h": 1,
            "24h": 24,
            "3d": 72,
            "7d": 168,
            "30d": 720,
            "90d": 2160,
            "120d": 2880,
            "1y": 8760,
            "all_time": len(df),
        }

        annualized_average_funding_rate = {}

        for timeframe, average_window in timeframes_preset.items():
            average_window = max(1, average_window)
            daily_rate = df["funding_rate"].head(average_window).mean() * (24 / self.funding_interval)
            annualized_rate = daily_rate * 365
            annualized_average_funding_rate[timeframe] = annualized_rate

        return {
            "exchange": "dydx",
            "market": market,
            "annualized_average_funding_rate": annualized_average_funding_rate,
        }
    
    def fetch_funding_rate_history_until_start(self, symbol):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/dydx/{symbol}")

        cache = self._load_cache(folder_path)

        latest_time_in_cache = None
        if cache:
            latest_time_in_cache = datetime.strptime(
                cache[0]["effectiveAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        start_time = datetime.now()
        new_data = []
        while True:
            response_data = self._fetch_funding_rate_history(symbol, start_time)

            if not response_data:
                break

            data = response_data["historicalFunding"]

            if len(data) == 0:
                break

            response_oldest_time = datetime.strptime(
                data[-1]["effectiveAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
            )

            # Break if we've reached the point in time where our cache starts
            if (
                latest_time_in_cache
                and response_oldest_time.timestamp() <= latest_time_in_cache.timestamp()
            ):
                break

            if start_time.timestamp() == response_oldest_time.timestamp():
                break

            new_data.extend(data)
            start_time = response_oldest_time

        if latest_time_in_cache:
            new_data = [
                entry
                for entry in new_data
                if datetime.strptime(
                    entry["effectiveAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).timestamp()
                > latest_time_in_cache.timestamp()
            ]

        data = new_data + cache

        self._save_cache(data, folder_path)

        return self.format_funding_rate_history(data)
    
    # Format functions
    def format_funding_rate_history(self, data):
        df = pd.DataFrame(data, columns=["effectiveAt", "rate"])

        df["funding_time"] = df["effectiveAt"]
        df["funding_rate"] = df["rate"].astype(float)

        df.sort_values(by=["funding_time"], ascending=False, inplace=True)

        return df[['funding_time', 'funding_rate']]


    # Private functions
    def _init_markets(self):
        markets_info = self._fetch_markets()
        for market, value in markets_info.items():
            self.markets_base[market] = value['baseAsset']

    def _fetch_markets(self):
        url = "https://api.dydx.exchange/v3/markets"

        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['markets']
        else:
            print(f"Error: {response.status_code}")
            return []

    def _fetch_24h_vol(self, symbol=None):
        url = f"https://api.dydx.exchange/v3/stats/{symbol}"

        response = requests.get(url)

        if response.status_code == 200:
            return response.json()["markets"][symbol]
        else:
            print(f"Error: {response.status_code}")
            return None

    def _load_cache(self, folder_path):
        cache_files = sorted(glob(f"{folder_path}/*.json"), reverse=True)
        data = []
        for file in cache_files:
            with open(file, "r") as f:
                data.extend(json.load(f))
        return data

    def _save_cache(self, data, folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        RECORDS_PER_FILE = 1000
        for i in range(0, len(data), RECORDS_PER_FILE):
            chunk = data[i : i + RECORDS_PER_FILE]
            filename = f"{folder_path}/{chunk[0]['effectiveAt']}.json"
            with open(filename, "w") as f:
                json.dump(chunk, f)

    def _fetch_funding_rate_history(self, symbol, start_time=None):
        url = f"https://api.dydx.exchange/v3/historical-funding/{symbol}"

        params = {
            "limit": 100
        }

        if start_time is not None:
            params["effectiveBeforeOrAt"] = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"DYDX {symbol} Error: {response.status_code}")
            return None
