import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import calendar

class ZetaFetcher:
    funding_interval = 1
    markets_base = {
        "BTC": "BTC",
        "ETH": "ETH",
        "SOL": "SOL",
        "APT": "APT",
        "ARB": "ARB"
    }

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
        # TODO: Implement this when Zeta exposes API
        return {
            "exchange": "zeta",
            "market": market,
            "timestamp": 0,
            "volume": float(0),
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
            "exchange": "zeta",
            "market": market,
            "annualized_average_funding_rate": annualized_average_funding_rate,
        }
    
    def fetch_funding_rate_history_until_start(self, symbol):
        timestamps = []
        funding_rates = []
        cur = datetime.now()
        while True:
            data = self._fetch_funding_rate_history_by_month(symbol, cur.year, cur.month)
            if not data or len(data['t']) == 0:
                break
            timestamps.extend(data['t'])
            funding_rates.extend(data['o'])
            cur = cur - timedelta(days=calendar.monthrange(cur.year, cur.month)[1])
        data = {
            "timestamp": timestamps,
            "funding_rate": funding_rates
        }
        return self._format_funding_rate_history(data)
    
    # Format functions
    def _format_funding_rate_history(self, data):
        df = pd.DataFrame(data)

        df["funding_time"] = df["timestamp"]
        df["funding_rate"] = df["funding_rate"].astype(float) / 10000

        df.sort_values(by=['funding_time'], ascending=False, inplace=True)
        
        return df[['funding_time', 'funding_rate']]

    # Private functions
    def _init_markets(self):
        pass
    
    def _fetch_24h_vol(self, symbol=None):
        return None

    def _fetch_funding_rate_history_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/zeta/{symbol}")
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

    def _fetch_funding_rate_history(
        self, symbol, start_time=None, end_time=None, limit=1000
    ):
        url = f"https://dex-funding-rate-mainnet.zeta.markets/tv/history"
        params = {
            "symbol": f"{symbol}-PERP-FUNDING",
            "resolution": "60",
            "countback": limit
        }
        if start_time is not None:
            params["from"] = int(start_time)  # Convert to ms
        if end_time is not None:
            params["to"] = int(end_time)  # Convert to ms

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Zeta {symbol} Error: {response.status_code}")
            return None
