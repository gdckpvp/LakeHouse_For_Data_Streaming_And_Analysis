import json
from datetime import datetime
from kafka import KafkaProducer
from websocket import create_connection, WebSocketConnectionClosedException
from time import sleep
import logging

# sleep(300)
# WebSocket URL
ws_url = "wss://ws.coincap.io/prices?assets=bitcoin,ethereum"
# Kafka configuration
kafka_broker = "broker:9092"
topic_ethereum = "ethereum"
topic_solana = "solana"
topic_xrp = "xrp"
topic_bitcoin = "bitcoin"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

def add_timestamp(data):
    now = datetime.now().timestamp()
    data["timestamp"] = now
    return data

def main():
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        api_version='7.0.1',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks = 'all'
    )
    while True:
        try:
            ws = create_connection(ws_url)
            logger.info("Connected to WebSocket")
            while True:
                data = json.loads(ws.recv())
                logger.info(data)
                if "bitcoin" in data:
                    value = add_timestamp({"bitcoin": data["bitcoin"]})
                    producer.send(topic_bitcoin, value=value)
                if "ethereum" in data:
                    value = add_timestamp({"ethereum": data["ethereum"]})
                    producer.send(topic_ethereum, value=value)
                break
            sleep(3)

        except WebSocketConnectionClosedException as e:
            print("WebSocket connection closed: ", e)
            print("Attempting to reconnect in 5 seconds...")
            sleep(10)
        except Exception as e:
            print("An error occurred: ", e)
            sleep(10)

if __name__ == "__main__":
    main()
