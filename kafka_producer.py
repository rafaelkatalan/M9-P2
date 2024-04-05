from confluent_kafka import Producer
import json
import random
import datetime
import time

# Kafka Configuration
kafka_broker = 'localhost:29092'

def message_receiving_simulation():
    id_sensor = "sensor_001"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tipo_poluente = "PM2.5"
    nivel = random.uniform(0, 100)
    json_data = {
        "idSensor": id_sensor,
        "timestamp": timestamp,
        "tipoPoluente": tipo_poluente,
        "nivel": nivel
    }
    return (json_data)
    

def on_message(msg, producer):
    try:
        message = str(msg)
        producer.produce('data', value=message.encode('utf-8'), callback=delivery_report)
        producer.flush()
        time.sleep(1)
    except Exception as e:
        print("Error:", e)

# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': kafka_broker})

def main():   
    message = message_receiving_simulation()
    on_message(message, producer)

if __name__ == '__main__':
    while True:
        main()