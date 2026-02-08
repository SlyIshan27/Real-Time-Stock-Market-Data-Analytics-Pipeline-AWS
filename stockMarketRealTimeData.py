import yfinance as yf
import time
import json
import csv
import boto3

#Get stock market data from an API, and then send to Kinesis.
def getRealTimeData(symbol, client):
    try:
        
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")
        
        
        if data.empty:
            print("No data found.")
            return
        latest = data.iloc[-1]

        record = {
            "symbol": symbol,
            "timestamp": str(data.index[-1]),
            "open": float(latest["Open"]),
            "high": float(latest["High"]),
            "low": float(latest["Low"]),
            "close": float(latest["Close"]),
            "volume": int(latest["Volume"])
        }
        
        response = client.put_record(
            StreamName = "Real-Time-Stock-Data-Stream",
            Data = json.dumps(record),
            PartitionKey = symbol)
        
        print("Sent to kinesis ", response)
            
    except Exception as e:
        print(f"Error fetching data: {e}")

ticker = "AAPL"
print(f"Tracking {ticker}...")
client = boto3.client('kinesis')
while True:
    getRealTimeData(ticker, client)
    time.sleep(60)
