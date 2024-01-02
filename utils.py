import os
import json

def fill_drift_missing_prices(binance_symbol, drift_symbol, year, month):
    dirname = os.path.dirname(__file__)
    
    folder_path = os.path.join(dirname, f"./modules/data/binance/prices/{binance_symbol}")
    file_path = os.path.join(dirname, f"{folder_path}/{binance_symbol}_{year}_{month}.json")
    
    with open(file_path, "r") as f:
        binance_price = json.load(f)
    
    drift_price = [{
        "start": str(item[0]),
        "open": item[1],
        "close": item[4],
        "high": item[2],
        "low": item[3],
        "quoteVolume": item[7],
        "baseVolume": item[5],
        "resolution": "60",
        "recordKey": str(item[0])
    } for item in binance_price]
    
    folder_path = os.path.join(dirname, f"./modules/data/drift/prices/{drift_symbol}")
    file_path = os.path.join(dirname, f"{folder_path}/{drift_symbol}_{year}_{month}.json")
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    with open(file_path, "w") as f:
        json.dump(drift_price, f)
        
    return drift_price

if __name__ == "__main__":
    months = [10, 11, 12]
    binance_symbols = ["BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    drift_symbols = ["BNB-PERP", "BTC-PERP", "ETH-PERP", "SOL-PERP", "XRP-PERP"]

    for (index, symbol) in enumerate(binance_symbols):
        for month in months:
            fill_drift_missing_prices(symbol, drift_symbols[index], 2023, month)

