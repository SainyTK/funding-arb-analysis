import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import calendar

class BinanceFetcher:

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
            "exchange": "binance",
            "market": market,
            "timestamp": raw["closeTime"],
            "volume": float(raw["quoteVolume"]) if raw else 0,
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
            '1y': 8760,
            'all_time': len(df)
        }

        annualized_average_funding_rate = {}

        for timeframe, average_window in timeframes_preset.items():
            average_window = max(1, average_window)
            daily_rate = df["funding_rate"].head(average_window).mean() * (24 / self.funding_interval)
            annualized_rate = daily_rate * 365
            annualized_average_funding_rate[timeframe] = annualized_rate

        return {
            "exchange": "binance",
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
            cur = cur - timedelta(days=cur.day)
        return self._format_funding_rate_history(result)
    
    def fetch_hourly_ohlc(self, symbol, start_time, end_time):
        result = []
        cur = datetime.fromtimestamp(end_time)
        while True:
            data = self._fetch_hourly_ohlc_by_month(symbol, cur.year, cur.month)
            if not data or cur.timestamp() < start_time:
                break
            result.extend(data)
            cur = cur - timedelta(days=cur.day)
        return self._format_ohlc(result)
    
    # Format functions
    def _format_funding_rate_history(self, data):
        df = pd.DataFrame(data, columns=["fundingTime", "fundingRate"])

        df["funding_rate"] = df["fundingRate"].astype(float)

        df['datetime'] = pd.to_datetime(df['fundingTime'], unit='ms')
        df['timestamp'] = df['datetime'].apply(lambda x: x.timestamp())
        df.sort_values(by=["datetime"], ascending=True, inplace=True)
        df.reset_index(inplace=True, drop=True)

        return df[["datetime", "timestamp", "funding_rate"]]
    
    def _format_ohlc(self, data):
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "x1", "x2", "x3", "x4", "x5", "x6", "x7"])

        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['timestamp'] = df['datetime'].apply(lambda x: x.timestamp())

        df.sort_values(by=["datetime"], ascending=True, inplace=True)
        return df[['datetime', 'timestamp', 'open', 'high', 'low', 'close']]

    # Private functions
    def _init_markets(self):
        markets = self._fetch_markets()
        for market in markets:
            self.markets_base[market["symbol"]] = market['baseAsset']

    def _fetch_markets(self):
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()["symbols"]
        else:
            print(f"Error: {response.status_code}")
            return []
        
    def _fetch_24h_vol(self, symbol=None):
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"

        if symbol is not None:
            params = {"symbol": symbol}
        else:
            params = {}

        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None

    def _fetch_funding_rate_history_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/binance/funding/{symbol}")
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
        end_time = (datetime(year, month, 1) + timedelta(days=num_days_in_month)).timestamp()
        end_time = min(end_time, now)

        data = self._fetch_funding_rate_history(symbol, start_time, end_time)

        if data:
            with open(file_path, "w") as f:
                json.dump(data, f)

        return data
    
    def _fetch_hourly_ohlc_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/binance/prices/{symbol}")
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
        end_time = (datetime(year, month, 1) + timedelta(days=num_days_in_month)).timestamp()
        end_time = min(end_time, now)

        data = self._fetch_ohlc(symbol, "1h", start_time, end_time)

        if data:
            with open(file_path, "w") as f:
                json.dump(data, f)

        return data

    def _fetch_funding_rate_history(
        self, symbol, start_time=None, end_time=None, limit=100
    ):
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        params = {
            "symbol": symbol,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = int(start_time * 1000)  # Convert to ms
        if end_time is not None:
            params["endTime"] = int(end_time * 1000)  # Convert to ms

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Binance {symbol} Error: {response.status_code}")
            return None
        
    def _fetch_ohlc(self, symbol, timeframe, start_time, end_time):
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": 1000
        }
        if start_time is not None:
            params["startTime"] = int(start_time * 1000)  # Convert to ms
        if end_time is not None:
            params["endTime"] = int(end_time * 1000)  # Convert to ms

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Binance {symbol} Error: {response.status_code}")
            return None
