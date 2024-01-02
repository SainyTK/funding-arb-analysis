import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import calendar


class HyperLiquidFetcher:
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
        last = raw[-1]
        return {
            "exchange": "hyperliquid",
            "market": market,
            "timestamp": last["t"],
            "volume": float(last["v"]) if last else 0,
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
            "exchange": "hyperliquid",
            "market": market,
            "annualized_average_funding_rate": annualized_average_funding_rate,
        }
    
    def fetch_funding_rate_history_until_start(self, symbol):
        result = []
        cur = datetime.now()
        while True:
            data = self._fetch_funding_rate_history_by_month(symbol, cur.year, cur.month)
            if not data:
                break
            result.extend(data)
            cur = cur - timedelta(days=calendar.monthrange(cur.year, cur.month)[1])
        return self._format_funding_rate_history(result)
    
    # Format functions
    def _format_funding_rate_history(self, data):
        df = pd.DataFrame(data, columns=["time", "fundingRate"])

        df['funding_time'] = df["time"]
        df["funding_rate"] = df["fundingRate"].astype(float)

        df.sort_values(by=["funding_time"], ascending=False, inplace=True)
        return df[["funding_time", "funding_rate"]]

    # Private functions
    def _init_markets(self):
        markets = self._fetch_markets()
        for market in markets:
            self.markets_base[market["name"]] = market['name']

    def _fetch_markets(self):
        url = "https://api.hyperliquid.xyz/info"
        body = {
            "type": "meta"
        }
        response = requests.post(url, json=body)
        if response.status_code == 200:
            return response.json()["universe"]
        else:
            print(f"Error: {response.status_code}")
            return []
        
    def _fetch_24h_vol(self, symbol):
        url = "https://api.hyperliquid.xyz/info"

        now = int(datetime.now().timestamp() * 1000)
        start_time = now - 24 * 60 * 60 * 1000

        data = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": "1d",
                "startTime": start_time,
                "endTime": now,
            },
        }

        response = requests.post(url, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None

    def _fetch_funding_rate_history_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/hyperliquid/{symbol}")
        file_path = os.path.join(dirname, f"{folder_path}/{symbol}_{year}_{month}.json")

        now = datetime.now()
        file_existed = os.path.exists(file_path)
        is_same_month = now.year == year and now.month == month

        if file_existed and not is_same_month:
            with open(file_path, "r") as f:
                data = json.load(f)
            return data

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        now = datetime.now().timestamp()
        start_time = datetime(year, month, 1).timestamp()

        # Get the number of days in the given month and year
        num_days_in_month = calendar.monthrange(year, month)[1]
        end_time = (
            datetime(year, month, 1) + timedelta(days=num_days_in_month)
        ).timestamp()
        end_time = min(end_time, now)

        data = self._fetch_funding_rate_history(symbol, start_time, end_time)

        if data:
            with open(file_path, "w") as f:
                json.dump(data, f)

        return data

    def _fetch_funding_rate_history(self, symbol, start_time, end_time=None):
        url = "https://api.hyperliquid.xyz/info"

        data = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": int(start_time * 1000),
            "endTime": int(end_time * 1000),
        }

        response = requests.post(url, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Hyperliquid {symbol} Error: {response.status_code}")
            return None
