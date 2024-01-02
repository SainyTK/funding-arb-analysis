import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import calendar

class GateIOFetcher:  

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
            "exchange": "gate",
            "market": market,
            "timestamp": int(datetime.timestamp(datetime.now()) * 1000),
            "volume": float(raw[0]['volume_24h_base']) if raw else 0,
        }
    
    def fetch_annualized_average_funding_rate(self, market):
        df = self.fetch_funding_rate_history_until_start(market)

        timeframes_preset = {
            '1h': 1,
            '24h': 24,
            '3d': 72,
            '7d': 168,
            '30d': 720,
            '90d': 2160,
            '120d': 2880,
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
            "exchange": "gate",
            "market": market,
            "annualized_average_funding_rate": annualized_average_funding_rate
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
        df = pd.DataFrame(data, columns=['t', 'r'])

        df['funding_time'] = df['t']
        df['funding_rate'] = df['r'].astype(float)

        df.sort_values(by=["funding_time"], ascending=False, inplace=True)
        return df[["funding_time", "funding_rate"]]

    # Private functions
    def _init_markets(self):
        markets = self._fetch_markets()
        for market in markets:
            self.markets_base[market["name"]] = market['name'].split('_')[0]

    def _fetch_markets(self):
        host = "https://api.gateio.ws"
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        url = '/futures/usdt/contracts'
        query_param = ''

        response = requests.get(host + prefix + url + query_param, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return []
        
    def _fetch_24h_vol(self, symbol=None):
        host = "https://api.gateio.ws"
        prefix = "/api/v4"
        url = '/futures/usdt/tickers'
        query_param = f'?contract={symbol}'
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        response = requests.get(host + prefix + url + query_param, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None
        
    def _fetch_funding_rate_history_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/gate/{symbol}")
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
        
    def _fetch_funding_rate_history(self, symbol, start_time=None, end_time=None,):
        url = "https://api.gateio.ws/api/v4/futures/usdt/funding_rate"

        params = {
            "contract": symbol,
        }
        if start_time is not None:
            params["from"] = int(start_time)
        if end_time is not None:
            params["to"] = int(end_time)
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Gate {symbol} Error: {response.status_code}")
            return None
