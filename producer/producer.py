import time
import requests
import json
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# =========================
# LOAD ENV VARIABLES
# =========================
load_dotenv()

API_KEY = os.getenv("ETHERSCAN_API_KEY")
WALLET = os.getenv("WALLET_ADDRESS")

if not API_KEY or not WALLET:
    raise ValueError("❌ Missing API_KEY or WALLET in .env")

URL = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=txlist&address={WALLET}&startblock=0&endblock=99999999&sort=desc&apikey={API_KEY}"

# =========================
# CONNECT TO KAFKA
# =========================
print("⏳ Connecting to Kafka...")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        print("✅ Connected to Kafka")
        break
    except NoBrokersAvailable:
        print("⏳ Kafka not ready, retrying...")
        time.sleep(5)

# =========================
# MAIN LOOP
# =========================
seen_tx = set()

while True:
    try:
        response = requests.get(URL, timeout=15)

        # Handle HTTP errors
        if response.status_code != 200:
            print(f"❌ HTTP Error: {response.status_code}")
            time.sleep(10)
            continue

        data = response.json()

        # Better debug output
        if data.get("status") != "1" or not data.get("result"):
            print("⚠️ API issue:", data)
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

        # Prevent memory overflow
        if len(seen_tx) > 2000:
            seen_tx.clear()

        time.sleep(10)

    except Exception as e:
        print("❌ Error:", str(e))
        time.sleep(10)
