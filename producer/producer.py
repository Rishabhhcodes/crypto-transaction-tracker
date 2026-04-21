import time
import requests
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

API_KEY = "YOUR_API_KEY"

WALLET = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"

URL = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=txlist&address={WALLET}&startblock=0&endblock=99999999&sort=desc&apikey={API_KEY}"

print("⏳ Connecting to Kafka...")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka")
        break
    except NoBrokersAvailable:
        print("⏳ Kafka not ready, retrying...")
        time.sleep(5)

seen_tx = set()

while True:
    try:
        response = requests.get(URL, timeout=20)
        data = response.json()

        if data.get("status") != "1" or not data.get("result"):
            print("⚠️ API issue:", data.get("message"))
            time.sleep(10)
            continue

        transactions = data["result"][:15]

        new_count = 0

        for tx in transactions:
            tx_hash = tx["hash"]

            if tx_hash in seen_tx:
                continue

            seen_tx.add(tx_hash)

            value_wei = int(tx["value"])
            value_eth = value_wei / 1e18

            message = {
                "tx_hash": tx_hash,
                "from_address": tx["from"],
                "to_address": tx["to"],
                "value": value_eth,
                "gas": int(tx["gas"]),
                "timestamp": int(tx["timeStamp"]),
                "is_token_tx": value_wei == 0
            }

            producer.send("crypto-topic", value=message)
            new_count += 1

        producer.flush()

        print(f"✅ Sent {new_count} transactions")

        if len(seen_tx) > 2000:
            seen_tx.clear()

        time.sleep(10)

    except Exception as e:
        print("❌ Error:", str(e))
        time.sleep(10)
