from confluent_kafka import Consumer
import json
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('./database.db')

# Create a table if it doesn't exist
conn.execute('''
    CREATE TABLE IF NOT EXISTS sensor_data (
        idSensor TEXT,
        timestamp TEXT,
        tipoPoluente TEXT,
        nivel REAL
    )
''')
conn.commit()
conn.close()
kafka_broker = 'localhost:29092'
kafka_topic = 'data'

consumer = Consumer({
    'bootstrap.servers': kafka_broker,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([kafka_topic])

conn = sqlite3.connect('./database.db')
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    message = msg.value().decode('utf-8')
    message = message.replace("'", "\"")
    json_data = json.loads(message)
    conn = sqlite3.connect('./database.db')
    print(message)
    conn.execute('''
                    INSERT INTO sensor_data (idSensor, timestamp, tipoPoluente, nivel)
                    VALUES (?, ?, ?, ?)
                 ''', (json_data["idSensor"], json_data["timestamp"], json_data["tipoPoluente"], json_data["nivel"]))
    conn.commit()
    conn.close()
       
consumer.close()
