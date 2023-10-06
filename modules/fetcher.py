from .exchanges.binance import BinanceFetcher
from .exchanges.gate import GateIOFetcher
from .exchanges.okx import OKXFetcher
from .exchanges.huobi import HuobiFetcher
from .exchanges.bitmex import BitmexFetcher
from .exchanges.drift import DriftMarketFetcher
from .exchanges.dydx import DYDXFetcher
from .exchanges.kwenta import KwentaMarketFetcher
from .exchanges.apollox import ApolloxFetcher
from .exchanges.zeta import ZetaFetcher
from .exchanges.hyperliquid import HyperLiquidFetcher

class Fetcher:
    def __init__(self):
        self.exchanges = {
            'binance': BinanceFetcher(),
            'gate': GateIOFetcher(),
            'okx': OKXFetcher(),
            'huobi': HuobiFetcher(),
            'bitmex': BitmexFetcher(),
            'drift': DriftMarketFetcher(),
            'dydx': DYDXFetcher(),
            'kwenta': KwentaMarketFetcher(),
            'apollox': ApolloxFetcher(),
            'zeta': ZetaFetcher(),
            'hyperliquid': HyperLiquidFetcher(),
            # add more exchanges here
        }

    def list_markets(self, exchange):
        return self.exchanges[exchange].list_markets()
    
    def get_market_base(self, exchange, market):
        return self.exchanges[exchange].get_market_base(market)

    def fetch_24h_vol(self, exchange, market):
        return self.exchanges[exchange].fetch_24h_vol(market)
    
    def fetch_annualized_average_funding_rate(self, exchange, market):
        return self.exchanges[exchange].fetch_annualized_average_funding_rate(market)
    
    def fetch_funding_rate_history_until_start(self, exchange, market):
        return self.exchanges[exchange].fetch_funding_rate_history_until_start(market)
    
    def fetch_ohlc(self, exchange, market, start_time, end_time):
        return self.exchanges[exchange].fetch_hourly_ohlc(market, start_time, end_time)