# kafka/producer.py

import asyncio
import websockets
import json
from kafka import KafkaProducer
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Kafka config
KAFKA_TOPIC = "crypto_prices"
KAFKA_BROKER = "localhost:9092"

# Binance WebSocket (BTC/USDT trade stream)
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def stream_to_kafka():
    async with websockets.connect(BINANCE_WS_URL) as websocket:
        logging.info("Connected to Binance WebSocket API")
        while True:
            try:
                msg = await websocket.recv()
                data = json.loads(msg)

                # Parse price and timestamp
                event = {
                    "symbol": data["s"],                        # e.g., BTCUSDT
                    "price_usd": float(data["p"]),              # e.g., "62341.05"
                    "timestamp": datetime.utcnow().isoformat()  # ISO timestamp
                }

                producer.send(KAFKA_TOPIC, value=event)
                logging.info(f"Sent to Kafka: {event}")

            except Exception as e:
                logging.error(f"Error: {e}")
                await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(stream_to_kafka())
