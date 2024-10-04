from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
import pandas as pd
import json

conf = {
    'bootstrap.servers': 'KAFKA_HOST:9092',
    'group.id': 'group-id',
    'auto.offset.reset': 'earliest', 
}

consumer = Consumer(conf)

topic = 'TOPIC'
consumer.subscribe([topic])

messages = []

no_message_count = 0 
max_no_message_count = 3

try:
    while True:
        msg = consumer.poll(timeout=10.0)  
        if msg is None:
            print("no message, polling again")
            no_message_count += 1
            if no_message_count >= max_no_message_count:
                print("no message, end of poll")
                break
            continue

        no_message_count = 0

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        message_value = msg.value().decode('utf-8')
        messages.append(json.loads(message_value) )

except KeyboardInterrupt:
    print("stop by key board interrupt")

finally:
    consumer.close()

df = pd.json_normalize(messages) 

current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'{topic}_{current_time}.csv'
df.to_csv(filename, index=False)
print(f"data has been exported to {filename}")