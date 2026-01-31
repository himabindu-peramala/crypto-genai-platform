import json
import time
import random
import requests
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'crypto_market_data'

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_crypto_data():
    """
    Fetches real-time crypto prices from CoinGecko API.
    """
    data = []
    
    # Real CoinGecko API
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'bitcoin,ethereum,solana',
        'vs_currencies': 'usd',
        'include_last_updated_at': 'true',
        'include_24hr_vol': 'true'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data_json = response.json()
        
        for coin_id, stats in data_json.items():
            payload = {
                'event_time': datetime.now().isoformat(),
                'symbol': coin_id.upper(),
                'price': float(stats['usd']),
                'volume': float(stats.get('usd_24h_vol', 0)),
                'source': 'coingecko_api'
            }
            data.append(payload)
            
    except Exception as e:
        print(f"Error fetching data: {e}. using cached/fallback if available.")
        # Optional: Add fallback logic or just skip
        
    return data

def run_producer():
    print(f"Starting REAL-TIME producer. Sending data to topic: {TOPIC_NAME}")
    try:
        while True:
            market_data = fetch_crypto_data()
            
            for transaction in market_data:
                producer.send(TOPIC_NAME, value=transaction)
                print(f"Sent: {transaction}")
            
            # Flush to ensure delivery
            producer.flush()
            
            # Rate Limit: CoinGecko free tier allows ~10-30 calls/min.
            # We sleep 15 seconds to be safe (4 calls/min).
            print("Sleeping 15s to respect API rate limits...")
            time.sleep(15)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()

if __name__ == "__main__":
    run_producer()
