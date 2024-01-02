import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import json
from .libs.kwenta.contracts import addresses, abis
from web3 import Web3
from dotenv import load_dotenv
import asyncio
from decimal import Decimal
import calendar

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

load_dotenv()


class KwentaMarketFetcher:

    funding_interval = 1

    funding_rate_persision = 9
    price_precision = 6

    markets: dict = {}
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
        if len(self.markets_base) == 0:
            self._init_markets()

        data = self._fetch_24h_vol(market)

        return {
            "exchange": "kwenta",
            "market": market,
            "timestamp": data['timestamp'],
            "volume": data['volume']
        }

    def fetch_annualized_average_funding_rate(self, market):
        if len(self.markets_base) == 0:
            self._init_markets()

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
            "exchange": "kwenta",
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
    def _format_funding_rate_history(self, raw_data):
        result = []
        for item in raw_data:
            result.append({
                "timestamp": int(item['timestamp']),
                "funding_rate": float(Decimal(item['fundingRate']) / Decimal(10**18) / Decimal(24))
            })

        df = pd.DataFrame(result, columns=['timestamp', 'funding_rate'])
        df.dropna(inplace=True)

        df['funding_time'] = df['timestamp'].astype(int)
        df['funding_rate'] = df['funding_rate'].astype(float)

        df.sort_values(by=['funding_time'], ascending=False, inplace=True)
        
        return df[['funding_time', 'funding_rate']]

    # Private functions
    def _init_markets(self):
        markets = self._fetch_markets()
        for key, value in markets.items():
            self.markets[key] = value
            self.markets_base[key] = key

    def _fetch_markets(self):
        rpc_url = os.environ["KWENTA_RPC_URL"]
        web3 = Web3(Web3.HTTPProvider(rpc_url))

        network_id = 10
        marketdata_contract = web3.eth.contract(
            web3.to_checksum_address(
                addresses["PerpsV2MarketData"][network_id]
            ),
            abi=abis["PerpsV2MarketData"],
        )
        allmarketsdata = (
            marketdata_contract.functions.allProxiedMarketSummaries().call()
        )

        markets = {}

        for market in allmarketsdata:
            normalized_market = {
                "market_address": market[0],
                "asset": market[1].decode("utf-8").strip("\x00"),
                "key": market[2],
                "maxLeverage": market[3],
                "price": market[4],
                "marketSize": market[5],
                "marketSkew": market[6],
                "marketDebt": market[7],
                "currentFundingRate": market[8],
                "currentFundingVelocity": market[9],
                "takerFee": market[10][0],
                "makerFee": market[10][1],
                "takerFeeDelayedOrder": market[10][2],
                "makerFeeDelayedOrder": market[10][3],
                "takerFeeOffchainDelayedOrder": market[10][4],
                "makerFeeOffchainDelayedOrder": market[10][5],
            }
            token_symbol = market[2].decode("utf-8").strip("\x00")[1:-4]
            markets[token_symbol] = normalized_market

        return markets
        
    def _fetch_24h_vol(self, symbol=None):
        url = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-perps'
        query = """
            query (
                $last_id: ID!
                $market_key: Bytes!
                $min_timestamp: BigInt = 0
            ) {
                futuresAggregateStats(
                    where: {
                        id_gt: $last_id
                        marketKey: $market_key
                        period: 3600
                        timestamp_gte: $min_timestamp
                    }
                    first: 1000
                    orderBy: timestamp
                    orderDirection: desc
                ) {
                    id
                    marketKey
                    period
                    timestamp
                    volume
                }
            }
        """

        transport = AIOHTTPTransport(url=url)
        client = Client(transport=transport, fetch_schema_from_transport=True)
        query = gql(query)

        market_key = None if self.markets[symbol] is None else self.markets[symbol]['key']

        min_timestamp = int((datetime.now() - timedelta(days=1)).timestamp())
        variables = {
            "last_id": "",
            "market_key": market_key.hex(),
            "min_timestamp": min_timestamp
        }

        response = asyncio.run(self._gql_fetch(client, query, variables))

        if response['futuresAggregateStats']:
            return self._format_24h_volume(symbol, response['futuresAggregateStats'])
        else:
            print(f"Kwenta {symbol} Error")
            return None
        
    def _format_24h_volume(self, symbol, raw_data):
        # sume volume of all items
        sum_vol = 0
        for item in raw_data:
            sum_vol += float(item['volume'])
        price = self._fetch_asset_price(symbol)
        return {
            "timestamp": int(item['timestamp']),
            "volume": sum_vol / 1e18 / price,
        }
    
    def _fetch_asset_price(self, symbol):
        api_key = os.environ['COINMARKETCAP_API_KEY']
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': api_key
        }
        parameters = {
            'symbol': symbol,
            'convert': 'USD'
        }

        response = requests.get(url, headers=headers, params=parameters)

        if response.status_code == 200:
            price_data = response.json()
            price = price_data['data'][symbol]['quote']['USD']['price']
            return price
        else:
            raise Exception(f"Failed to fetch price for {symbol}. Status code: {response.status_code}")
    
    def _fetch_funding_rate_history_by_month(self, symbol, year, month):
        dirname = os.path.dirname(__file__)
        folder_path = os.path.join(dirname, f"../data/kwenta/{symbol}")
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
        self, symbol, start_time=0, end_time = int(datetime.now().timestamp()), limit=1000
    ):
        url = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-perps'
        query = """
            query (
                $last_id: ID!
                $market_key: Bytes!
                $min_timestamp: BigInt = 0
                $max_timestamp: BigInt!
                $limit: Int = 1000                      
            ) {
                fundingRatePeriods(
                    where: {
                        id_gt: $last_id
                        marketKey: $market_key
                        timestamp_gt: $min_timestamp
                        timestamp_lt: $max_timestamp
                        period: Hourly
                    }
                    first: $limit
                    orderBy: timestamp
                    orderDirection: asc
                ) {
                    id
                    period
                    asset
                    marketKey
                    fundingRate
                    timestamp
                }
            }
        """

        market_key = None if self.markets[symbol] is None else self.markets[symbol]['key']

        transport = AIOHTTPTransport(url=url)
        client = Client(transport=transport, fetch_schema_from_transport=True)
        query = gql(query)
        variables = {
            "last_id": "",
            "market_key": market_key.hex(),
            "min_timestamp": int(start_time),
            "max_timestamp": int(end_time),
            "limit": int(limit)
        }

        response = asyncio.run(self._gql_fetch(client, query, variables))

        if response['fundingRatePeriods']:
            return response['fundingRatePeriods']
        else:
            print(f"Kwenta {symbol} Error")
            return None
        
    async def _gql_fetch(self, client, query, variables={}):
        response = await client.execute_async(query, variable_values=variables)
        return response