import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer,
        acks='all'
    )
    
    order_id = 1001 
    topic_name = 'topic1_logs'

    print(f"Starting order stream to topic '{topic_name}'...")

    try:
        while True:
            order = {
                "order_id": order_id,
                "customer_id": random.randint(100, 500),
                "product_id": random.randint(1, 50),
                "quantity": random.randint(1, 10),
                "price": round(random.uniform(5.00, 500.00), 2),
                "order_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }

            producer.send(topic_name, value=order)
            print(f"Produced record: {order}")
            
            order_id += 1
            time.sleep(random.uniform(1.0, 2.0))
            
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
