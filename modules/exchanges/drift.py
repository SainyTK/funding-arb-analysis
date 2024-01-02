import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import json
import calendar

class DriftMarketFetcher:
    funding_interval = 1
    markets_base = {
        "BTC-PERP": "BTC",
        "ETH-PERP": "ETH",
        "SOL-PERP": "SOL",
        "XRP-PERP": "XRP",
        "DOGE-PERP": "DOGE",
        "APT-PERP": "APT",
        "RNDR-PERP": "RNDR",
        "SUI-PERP": "SUI",
        "ARB-PERP": "ARB",
        "BNB-PERP": "BNB",
        "OP-PERP": "OP",
        "MATIC-PERP": "MATIC",
        "1MBONK-PERP": "1MBONK",
        "1MPEPE-PERP": "1MPEPE",
    }

    funding_rate_persision = 9
    price_precision = 6

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

        current_time = datetime.now().timestamp() * 1000
        closest_data = None
        closest_time_diff = float("inf")

        for item in raw:
            item_time = int(item["start"])
            time_diff = abs(current_time - item_time)
            if time_diff < closest_time_diff:
                closest_time_diff = time_diff
                closest_data = item

        return {
            "exchange": "drift",
            "market": market,
            "timestamp": int(closest_data["start"]),
            "volume": float(closest_data["baseVolume"]) if closest_data else 0,
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
            "exchange": "drift",
            "market": market,
            "annualized_average_funding_rate": annualized_average_funding_rate,
        }
    
    def fetch_funding_rate_history_until_start(self, symbol):
        result = []
        cur = datetime.now()
        while True:
            data = self._fetch_funding_rate_history_by_month(
                symbol, cur.year, cur.month
            )
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
            if cur.timestamp() < start_time:
                break
            if data is not None:
                result.extend(data)
            cur = cur - timedelta(days=cur.day)
        return self._format_ohlc(result)
    
    # Format functions
    def _format_funding_rate_history(self, data):
        df = pd.DataFrame(data, columns=["ts", "fundingRate", "oraclePriceTwap"])
        df.dropna(inplace=True)

        df["funding_rate"] = (
            df["fundingRate"].astype(float) / np.power(10, self.funding_rate_persision)
        ) / (df["oraclePriceTwap"].astype(float) / np.power(10, self.price_precision))

        df["datetime"] = pd.to_datetime(df["ts"].astype(int), unit="s")
        df['timestamp'] = df['datetime'].apply(lambda x: x.timestamp())
        df.sort_values(by=["datetime"], ascending=True, inplace=True)
        df.reset_index(inplace=True, drop=True)

        return df[["datetime", "timestamp", "funding_rate"]]
    
    def _format_ohlc(self, data):
        df = pd.DataFrame(data, columns=["start", "open", "high", "low", "close", "fillOpen", "fillHigh", "fillLow", "fillClose"])

        df['start'] = df['start'].astype(int) * 1000 * 1000

        df["datetime"] = pd.to_datetime(df["start"])
        df["timestamp"] = df['datetime'].apply(lambda x: x.timestamp())

        df.loc[(df['open'] == "undefined") | (pd.isna(df['open'])), 'open'] = df['fillOpen']
        df.loc[(df['high'] == "undefined") | (pd.isna(df['high'])), 'high'] = df['fillHigh']
        df.loc[(df['low'] == "undefined") | pd.isna(df['low']), 'low'] = df['fillLow']
        df.loc[(df['close'] == "undefined") | pd.isna(df['close']), 'close'] = df['fillClose']

        df["open"] = df["open"].astype(float)
        df["high"] = df['high'].astype(float)
        df["low"] = df['low'].astype(float)
        df["close"] = df['close'].astype(float)

        df.sort_values(by=["datetime"], ascending=True, inplace=True)
        return df[['datetime', 'timestamp', 'open', 'high', 'low', 'close']]

    # Private function
    def _init_markets(self):
        pass

    def _fetch_24h_vol(self, symbol=None):
        url = f"https://drift-historical-data.s3.eu-west-1.amazonaws.com/program/dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH/market/{symbol}/candles/{datetime.now().year}/{datetime.now().month}/resolution/D"
        response = requests.get(url)

        if response.status_code == 200:
            lines = response.text.strip().split("\n")
            header = lines[0].split(",")
            data = []
            for line in lines[1:]:
                values = line.split(",")
                item = dict(zip(header, values))
                data.append(item)
            return data
        else:
            print(f"Error: {response.status_code}")
            return None

    def _fetch_funding_rate_history_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/drift/funding/{symbol}")
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

        data = self._fetch_funding_rate_history(symbol, year, month)

        if data:
            with open(file_path, "w") as f:
                json.dump(data, f)
        
        return data
    
    def _fetch_hourly_ohlc_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/drift/prices/{symbol}")
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

        data = self._fetch_ohlc(symbol, "60", year, month)

        if data:
            with open(file_path, "w") as f:
                json.dump(data, f)

        return data
        
    def _fetch_funding_rate_history(self, symbol, year, month):
        url = f"https://drift-historical-data.s3.eu-west-1.amazonaws.com/program/dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH/market/{symbol}/funding-rates/{year}/{month}"
        response = requests.get(url)

        if response.status_code == 200:
            lines = response.text.strip().split("\n")
            header = lines[0].split(",")
            data = []
            for line in lines[1:]:
                values = line.split(",")
                item = dict(zip(header, values))
                data.append(item)
            return data
        else:
            print(f"Drift {symbol} Error: {response.status_code}")
            return None
        
    def _fetch_ohlc(self, symbol, timeframe, year, month):
        print(symbol, timeframe, year, month)
        url = f"https://drift-historical-data.s3.eu-west-1.amazonaws.com/program/dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH/market/{symbol}/candles/{year}/{month}/resolution/{timeframe}"
        response = requests.get(url)

        if response.status_code == 200:
            lines = response.text.strip().split("\n")
            header = lines[0].split(",")
            data = []
            for line in lines[1:]:
                values = line.split(",")
                item = dict(zip(header, values))
                data.append(item)
            return data
        else:
            print(f"Drift {symbol} Error: {response.status_code}")
            return None

