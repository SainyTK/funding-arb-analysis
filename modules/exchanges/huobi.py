import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import json
from glob import glob

class HuobiFetcher:

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

        record = raw["data"][0]

        return {
            "exchange": "huobi",
            "market": market,
            "timestamp": raw["ts"],
            "volume": float(record["vol"]) if record else 0,
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
            "exchange": "huobi",
            "market": market,
            "annualized_average_funding_rate": annualized_average_funding_rate,
        }
    
    def fetch_funding_rate_history_until_start(self, symbol):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/huobi/{symbol}")
        
        cache = self.load_cache(folder_path)

        latest_time_in_cache = None
        if cache:
            latest_time_in_cache = cache[0]['funding_time']

        page_index = 1
        new_data = []
        while True:
            response_data = self._fetch_funding_rate_history(symbol, page_index)

            if not response_data:
                break

            # Break if we've reached the point in time where our cache starts
            if latest_time_in_cache is not None and response_data[-1]['funding_time'] <= latest_time_in_cache:
                break

            new_data.extend(response_data)
            page_index += 1
        
        if latest_time_in_cache:
            new_data = [entry for entry in new_data if entry['funding_time'] > latest_time_in_cache]

        data = new_data + cache

        self.save_cache(data, folder_path)

        return self._format_funding_rate_history(data)

    # Format functions
    def _format_funding_rate_history(self, data):
        df = pd.DataFrame(data, columns=["funding_time", "funding_rate"])

        df["funding_rate"] = df["funding_rate"].astype(float)

        df.sort_values(by=["funding_time"], ascending=False, inplace=True)
        return df[["funding_time", "funding_rate"]]
    
    # Private functions
    def _init_markets(self):
        markets = self._fetch_markets()
        for market in markets:
            self.markets_base[market["contract_code"]] = market['symbol']

    def _fetch_markets(self):
        url = 'https://api.hbdm.com/swap-api/v1/swap_contract_info'

        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data']
        else:
            print(f"Error: {response.status_code}")
            return []
        
    def _fetch_24h_vol(self, symbol=None):
        url = "https://api.hbdm.com/swap-ex/market/history/kline?period=1day&size=1"

        if symbol is not None:
            params = {"contract_code": symbol}
        else:
            params = {}

        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None
    
    def load_cache(self, folder_path):
        cache_files = sorted(glob(f'{folder_path}/*.json'), reverse=True)
        data = []
        for file in cache_files:
            with open(file, 'r') as f:
                data.extend(json.load(f))
        return data

    def save_cache(self, data, folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            
        RECORDS_PER_FILE = 1000
        for i in range(0, len(data), RECORDS_PER_FILE):
            chunk = data[i:i+RECORDS_PER_FILE]
            filename = f"{folder_path}/{chunk[0]['funding_time']}.json"
            with open(filename, 'w') as f:
                json.dump(chunk, f)

    def _fetch_funding_rate_history(
        self, symbol, page_index = 1, limit=50
    ):
        url = "https://api.hbdm.com/swap-api/v1/swap_historical_funding_rate"
        params = {
            "contract_code": symbol,
            "page_index": page_index,
            "page_size": limit,
        }
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()['data']['data']
        else:
            print(f"Huobi {symbol} Error: {response.status_code}")
            return None
