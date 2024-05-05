import pandas as pd
from modules.fetcher import Fetcher
import json
import numpy as np

# Util function for fetching data
def fetch_data(exchange, market):
    fetcher = Fetcher()
    funding_df = fetcher.fetch_funding_rate_history_until_start(exchange, market)
    funding_df['datetime'] = funding_df['datetime'].dt.tz_localize(None)

    start_funding_time = funding_df['timestamp'].min()
    end_funding_time = funding_df['timestamp'].max()

    price_df = fetcher.fetch_ohlc(exchange, market, start_funding_time, end_funding_time)
    price_df['datetime'] = price_df['datetime'].dt.tz_localize(None)

    start_price_time = price_df['timestamp'].min()
    end_price_time = price_df['timestamp'].max()

    min_time = max(start_funding_time, start_price_time)
    max_time = min(end_funding_time, end_price_time)

    min_datetime = pd.to_datetime(min_time, unit='s')
    max_datetime = pd.to_datetime(max_time, unit='s')

    funding_df = funding_df[(funding_df['datetime'] >= min_datetime) & (funding_df['datetime'] <= max_datetime)]
    price_df = price_df[(price_df['datetime'] >= min_datetime) & (price_df['datetime'] <= max_datetime)]

    result_df = pd.merge_asof(funding_df, price_df, on=['datetime'], tolerance=pd.Timedelta('1h'), direction='nearest')
    result_df['open'] = result_df['open'].ffill()
    result_df['high'] = result_df['high'].ffill()
    result_df['low'] = result_df['low'].ffill()
    result_df['close'] = result_df['close'].ffill()
    result_df['timestamp'] = result_df['timestamp_x']

    result_df['datetime'] = result_df['datetime'].apply(lambda x: pd.to_datetime(x))

    return result_df[['datetime', 'timestamp', 'open', 'high', 'low', 'close', 'funding_rate']]

def fetch_24h_vol(exchange, market):
    fetcher = Fetcher()
    vol = fetcher.fetch_24h_vol(exchange, market)

    return vol

# Util functions for calculating pnl (long spot, short future)
def get_backtest_result(input_df, l, fee = 0.001, maintenance_margin = 0.05, stop_loss_margin = 0.0625):
    df = input_df.copy()
    for index, row in enumerate(df.iterrows()):
        if index == 0:
            df['clt'] = float(1)
            df['leverage'] = l
            df['entry'] = float(0)
            df['pos_size'] = float(0)
            df['change'] = float(0)
            df['change_pnl'] = float(0)
            df['funding'] = float(0)
            df['funding_pnl'] = float(0)
            df['margin'] = float(0)

            df['mm'] = df['clt'] * df['leverage'] * maintenance_margin
            df['mm_sl'] = df['clt'] * df['leverage'] * stop_loss_margin

            df['is_liq'] = False
            df['is_sl'] = False

            df['fee'] = -fee * df['leverage']
            df['final_pnl'] = float(0)
        else:
            prev_df = df.loc[index - 1]
            # check if is there was a trade in the previous record
            traded = prev_df['fee'] != 0

            # calculate new clt from previous clt + fee + funding pnl (if traded)
            new_clt = prev_df['clt'] + prev_df['fee'] + (prev_df['funding_pnl'] if traded else 0)

            if new_clt == 0:
                df.loc[index, 'clt'] = float(0)
                df.loc[index, 'entry'] = float(0)
                df.loc[index, 'pos_size'] = float(0)
                df.loc[index, 'change'] = float(0)
                df.loc[index, 'change_pnl'] = float(0)
                df.loc[index, 'funding'] = float(0)
                df.loc[index, 'funding_pnl'] = float(0)
                df.loc[index, 'margin'] = float(0)
                df.loc[index, 'mm'] = float(0)
                df.loc[index, 'mm_sl'] = float(0)
                df.loc[index, 'is_liq'] = False
                df.loc[index, 'is_sl'] = False
                df.loc[index, 'fee'] = float(0)
                df.loc[index, 'final_pnl'] = -1
            else:
                price = float(df.loc[index, 'close'])
                funding_rate = float(df.loc[index, 'funding_rate'])

                df.loc[index, 'clt'] = max(new_clt, float(0))
                # Entry price of the current position
                df.loc[index, 'entry'] = price if traded else prev_df['entry']
                # Size of the current position
                df.loc[index, 'pos_size'] = price * df.loc[index, 'clt'] * df.loc[index, 'leverage']
                # Change of the current position (compared to entry price)
                df.loc[index, 'change'] = (price - df.loc[index, 'entry']) / df.loc[index, 'entry'] if df.loc[index, 'entry'] != 0 else 0
                # Change pnl (we treat any changes as loss since we will either close the short position or long position if the price hits the stop loss)
                df.loc[index, 'change_pnl'] = -abs(df.loc[index, 'change'] * df.loc[index, 'leverage'])
                # Funding payment comes from the funding rate and the position size (collateral + change pnl)
                df.loc[index, 'funding'] = (df.loc[index, 'clt'] - df.loc[index, 'change'] / 2) * funding_rate * df.loc[index, 'leverage'] / 2
                # Funding pnl is accumulated while there is no trading. If there is a trade, we reset the funding pnl and set it to the current funding payment
                df.loc[index, 'funding_pnl'] = df.loc[index, 'funding'] if traded else df.loc[index, 'funding'] + df.loc[index - 1, 'funding_pnl']
                # Calculate current margin to check stop loss and liquidation
                df.loc[index, 'margin'] = (df.loc[index, 'clt'] + df.loc[index, 'change_pnl'] + df.loc[index, 'funding_pnl']) / df.loc[index, 'clt'] if df.loc[index, 'clt'] != 0 else 0

                # Calculate maintenance margin for liquidation
                df.loc[index, 'mm'] = df.loc[index, 'clt'] * df.loc[index, 'leverage'] * maintenance_margin
                # Calculate maintenance margin for stop loss
                df.loc[index, 'mm_sl'] = df.loc[index, 'clt'] * df.loc[index, 'leverage'] * stop_loss_margin

                # Check if the current position is liquidated
                df.loc[index, 'is_liq'] = df.loc[index, 'margin'] < df.loc[index,'mm']
                # Check if the current position is stop loss
                df.loc[index, 'is_sl'] = df.loc[index, 'margin'] < df.loc[index, 'mm_sl']

                # Include fee if liquidation or stop loss occur
                df.loc[index, 'fee'] = -fee * df.loc[index, 'leverage'] if df.loc[index, 'is_liq'] or df.loc[index, 'is_sl'] else 0
                # Calculate final pnl
                df.loc[index, 'final_pnl'] = df.loc[index, 'clt'] - 1 + df.loc[index, 'funding_pnl']
    return df

# Util functions for calculating pnl (long + short futures)
def get_dual_backtest_result(long_df, short_df, long_funding_freq, short_funding_freq, leverage, init_clt = 1, fee_percent = 0.001, stop_loss_margin = 0.0625):

    df = pd.merge_asof(long_df, short_df, on='timestamp')

    long_df = df
    long_df[['datetime', 'close', 'funding_rate']] = df[['datetime_x', 'close_x', 'funding_rate_x']]
    long_df = long_df[['datetime', 'close', 'funding_rate']]
    long_df.loc[:, 'funding_rate'] = long_df['funding_rate']

    short_df = df
    short_df[['datetime', 'close', 'funding_rate']] = df[['datetime_y', 'close_x', 'funding_rate_y']] # Use the same price reference to avoid fluctuation
    short_df = short_df[['datetime', 'close', 'funding_rate']]
    short_df.loc[:, 'funding_rate'] = short_df['funding_rate'] * long_funding_freq / short_funding_freq

    for index, row in enumerate(long_df.iterrows()):
        if index == 0:
            long_df = init_backtest_df(long_df, init_clt / 2, leverage)
            short_df = init_backtest_df(short_df, init_clt / 2, leverage)
        else:
            prev_drift_row = long_df.loc[index - 1]
            prev_binance_row = short_df.loc[index - 1]

            first_trade = index == 1
            is_sl = prev_drift_row['is_sl'] or prev_binance_row['is_sl']

            if first_trade:
                long_df.loc[index] = make_trade(long_df.loc[index], long_df.loc[index - 1], fee_percent, stop_loss_margin, 'long', 0)
                short_df.loc[index] = make_trade(short_df.loc[index], short_df.loc[index - 1], fee_percent, stop_loss_margin, 'short', 0)
            elif is_sl:
                drift_margin = long_df.loc[index - 1, 'margin']
                binance_margin = short_df.loc[index - 1, 'margin']
                avg_margin = (drift_margin + binance_margin) / 2
                drift_inj = avg_margin - drift_margin
                binance_inj = avg_margin - binance_margin

                long_df.loc[index] = make_trade(long_df.loc[index], long_df.loc[index - 1], fee_percent, stop_loss_margin, 'long', drift_inj)
                short_df.loc[index] = make_trade(short_df.loc[index], short_df.loc[index - 1], fee_percent, stop_loss_margin, 'short', binance_inj)
            else:
                long_df.loc[index] = record_row(long_df.loc[index], long_df.loc[index - 1], stop_loss_margin)
                short_df.loc[index] = record_row(short_df.loc[index], short_df.loc[index - 1], stop_loss_margin)
    
    result_df = long_df.copy()
    result_df[['close', 'long_funding', 'long_pnl']] = long_df[['close', 'funding_rate', 'pnl']]
    result_df[['short_funding', 'short_pnl']] = short_df[['funding_rate', 'pnl']]

    result_df['datetime'] = long_df['datetime']
    result_df['final_pnl'] = long_df['pnl'] + short_df['pnl']
    result_df = result_df[['datetime', 'close', 'long_funding', 'short_funding', 'long_pnl', 'short_pnl', 'final_pnl']]

    return (result_df, long_df, short_df)

# Util functions for hodl pnl
def get_hodl_result(input_df):
    df = input_df.copy()
    df = df.sort_values(by='datetime', ascending=True)
    df = df.reset_index(drop=True)
    df['close'] = df['close'].astype(float)
    first_price = df.loc[0, 'close']
    df['pnl'] = (df['close'] - first_price) / first_price
    return df

# Util functions for max drawdown pnl
def max_drawdown(series: pd.Series):
    equity_curve = series + 1
    cumulative_max = equity_curve.cummax()
    drawdowns = (equity_curve - cumulative_max) / cumulative_max
    drawdowns.fillna(0, inplace=True)
    return drawdowns.min()

# Util functions for sharpe ratio calculation
def sharpe_ratio(values, invest_return, risk_free_rate = 0.01, hr_interval = 1):
    excess_returns = invest_return - risk_free_rate
    std_excess_return = values.std() * np.sqrt((24 / hr_interval) * (365 / 2)) # 6 month stdev from an hour timeframe
    return excess_returns / std_excess_return

# Util functions for managing cache data
def get_cache_path(exchange, market):
    return f'./data/{exchange}_{market}.csv'

def save_cache_data(exchange, market, data_df):
    return data_df.to_csv(get_cache_path(exchange, market), index=False)

def load_cache_data(exchange, market):
    return pd.read_csv(get_cache_path(exchange, market))

def load_volume_data():
    file_path = "./storage/volume.json"
    with open(file_path, "r") as f:
        data = json.load(f)
    return data

def init_backtest_df(df, clt, leverage):
    new_df = df.copy()
    new_df.loc[:, 'is_trade'] = False

    new_df.loc[:, 'inj'] = float(clt)
    new_df.loc[:, 'eq'] = float(clt)
    new_df.loc[:, 'clt'] = float(clt)

    new_df.loc[:, 'leverage'] = leverage
    new_df.loc[:, 'entry'] = float(0)
    new_df.loc[:, 'pos_size'] = float(0)
    new_df.loc[:, 'd'] = 0

    new_df.loc[:, 'change'] = float(0)
    new_df.loc[:, 'change_pnl'] = float(0)
    new_df.loc[:, 'funding'] = float(0)
    new_df.loc[:, 'funding_pnl'] = float(0)
    new_df.loc[:, 'margin'] = float(0)

    new_df.loc[:, 'mm_sl'] = float(0)

    new_df.loc[:, 'is_sl'] = False

    new_df.loc[:, 'fee'] = float(0)
    new_df.loc[:, 'pnl'] = float(0)

    return new_df

def make_trade(row, prev_row, fee_percent, stop_loss_margin, side, inj):
    new_row = row.copy()

    new_row['is_trade'] = True

    price = float(new_row['close'])
    d = 1 if side == 'long' else -1

    new_clt = prev_row['clt'] + prev_row['change_pnl'] + prev_row['funding_pnl'] + inj

    fee = new_clt * new_row['leverage'] * fee_percent
    new_clt = new_clt - fee

    new_row['inj'] = inj
    new_row['eq'] = prev_row['eq'] + inj

    new_row['clt'] = max(new_clt, 0)
    new_row['d'] = d
    new_row['entry'] = price
    new_row['pos_size'] = new_row['clt'] * prev_row['leverage'] * d / price

    new_row['change'] = 0
    new_row['change_pnl'] = 0
    new_row['funding'] = 0
    new_row['funding_pnl'] = 0

    new_row['margin'] = new_row['clt']
    new_row['mm_sl'] = new_row['clt'] * new_row['leverage'] * stop_loss_margin

    new_row['is_sl'] = new_row['margin'] < new_row['mm_sl']
    new_row['fee'] = fee

    new_row['pnl'] = new_row['margin'] - new_row['eq']
    return new_row

def record_row(row, prev_row, stop_loss_margin):
    new_row = row.copy()

    new_row['is_trade'] = False

    price = float(new_row['close'])
    funding_rate = float(new_row['funding_rate'])

    new_row['inj'] = 0
    new_row['eq'] = prev_row['eq'] + new_row['inj']

    new_row['clt'] = prev_row['clt']
    new_row['d'] = prev_row['d']
    new_row['entry'] = prev_row['entry']
    new_row['pos_size'] = prev_row['pos_size']

    new_row['change'] = (price - new_row['entry'])
    new_row['change_pnl'] = new_row['change'] * new_row['pos_size']

    new_row['funding'] = -funding_rate * new_row['pos_size'] * price
    new_row['funding_pnl'] = prev_row['funding_pnl'] + new_row['funding']

    new_row['margin'] = new_row['clt'] + new_row['change_pnl'] + new_row['funding_pnl']
    new_row['mm_sl'] = new_row['clt'] * new_row['leverage'] * stop_loss_margin

    new_row['is_sl'] = new_row['margin'] < new_row['mm_sl']
    new_row['fee'] = 0

    new_row['pnl'] = new_row['margin'] - new_row['eq']
    return new_row