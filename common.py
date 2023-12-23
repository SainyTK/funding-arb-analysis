import pandas as pd
from modules.fetcher import Fetcher

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

# Util functions for calculating pnl
def get_backtest_result(input_df, l, fee = 0.001, maintenance_margin = 0.05, stop_loss_margin = 0.0625):
    df = input_df.copy()
    for index, row in enumerate(df.iterrows()):
        if index == 0:
            df['clt'] = 1
            df['leverage'] = l
            df['entry'] = 0
            df['pos_size'] = 0
            df['change'] = 0
            df['change_pnl'] = 0
            df['funding'] = 0
            df['funding_pnl'] = 0
            df['margin'] = 0

            df['mm'] = df['clt'] * df['leverage'] * maintenance_margin
            df['mm_sl'] = df['clt'] * df['leverage'] * stop_loss_margin

            df['is_liq'] = False
            df['is_sl'] = False

            df['fee'] = -fee * df['leverage']
            df['final_pnl'] = 0
        else:
            prev_df = df.loc[index - 1]
            # check if is there was a trade in the previous record
            traded = prev_df['fee'] != 0

            # calculate new clt from previous clt + fee + funding pnl (if traded)
            new_clt = prev_df['clt'] + prev_df['fee'] + (prev_df['funding_pnl'] if traded else 0)

            if new_clt == 0:
                df.loc[index, 'clt'] = 0
                df.loc[index, 'entry'] = 0
                df.loc[index, 'pos_size'] = 0
                df.loc[index, 'change'] = 0
                df.loc[index, 'change_pnl'] = 0
                df.loc[index, 'funding'] = 0
                df.loc[index, 'funding_pnl'] = 0
                df.loc[index, 'margin'] = 0
                df.loc[index, 'mm'] = 0
                df.loc[index, 'mm_sl'] = 0
                df.loc[index, 'is_liq'] = False
                df.loc[index, 'is_sl'] = False
                df.loc[index, 'fee'] = 0
                df.loc[index, 'final_pnl'] = -1
            else:
                price = float(df.loc[index, 'close'])
                funding_rate = float(df.loc[index, 'funding_rate'])

                df.loc[index, 'clt'] = max(new_clt, 0)
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
def max_drawdown(values):
    # Calculate the running maximum
    running_max = values.expanding(min_periods=1).max()
    # Calculate the drawdown
    drawdowns = running_max - values
    drawdowns.replace(float('inf'), 0, inplace=True)

    return drawdowns.max()

# Util functions for managing cache data
def get_cache_path(exchange, market):
    return f'./data/{exchange}_{market}.csv'

def save_cache_data(exchange, market, data_df):
    return data_df.to_csv(get_cache_path(exchange, market), index=False)

def load_cache_data(exchange, market):
    return pd.read_csv(get_cache_path(exchange, market))