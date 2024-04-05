from kafka_producer import on_message
from confluent_kafka import Producer
import sqlite3
import subprocess
import time

def teste():
    subprocess.Popen('python3 /home/rafael/Documents/GitHub/M9-P2/kafka-consumer.py', shell=True)
    print("Consumer started")
    kafka_broker = 'localhost:29092'
    message = {
        "idSensor": "sensor_001",
        "timestamp": "2024-04-04T12:34:56Z",
        "tipoPoluente": "PM2.5",
        "nivel": 35.2
    }
    producer = Producer({'bootstrap.servers': kafka_broker})
    on_message(message, producer)
    # Connect to the SQLite database
    time.sleep(15)
    conn = sqlite3.connect('./database.db')
    cursor = conn.cursor()

    # Retrieve the last inserted message from the database
    cursor.execute("SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT 1")
    last_message = cursor.fetchone()

    # Compare value by value of the sent message with the message from the database
    if last_message is not None:
        if last_message[0] == message["idSensor"] and last_message[1] == message["timestamp"] and last_message[2] == message["tipoPoluente"] and last_message[3] == message["nivel"]:
            print("Messages match")
        else:
            print("Messages do not match", message, last_message)
    else:
        print("No message found in the database")

    # Close the database connection
    conn.close()

teste()