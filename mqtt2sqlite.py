import sqlite3
import sys
from time import time

import paho.mqtt.client as mqtt
import yaml


def on_connect_v1(mqtt_client, _user_data, _flags, _conn_result):
    mqtt_client.subscribe("shellies/#")


def on_connect(mqtt_client, _user_data, _flags, _conn_result, _properties):
    mqtt_client.subscribe("shellies/#")


def on_message(_mqtt_client, user_data, message):
    topic = message.topic
    device = topic.split("/")[1]
    if device.startswith("shelly1-") or device == "announce":
        return

    payload = message.payload.decode("utf-8")
    created_at = int(time())
    print(f"II: Store data >> {topic} {device} {payload} {created_at}")

    db_conn = user_data["db_conn"]
    cursor = db_conn.cursor()
    sql = "INSERT INTO shellies (topic, device, payload, created_at) VALUES (?, ?, ?, ?)"
    cursor.execute(sql, (topic, device, payload, created_at))
    db_conn.commit()
    cursor.close()


def main():
    with open(sys.argv[1], encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    mqtt_host = config["mqtt_host"]
    mqtt_port = config["mqtt_port"]
    database_file = config["database_file"]

    with sqlite3.connect(database_file) as db_conn:
        print(f"II: Connected to {database_file}")
        sql = """
        CREATE TABLE IF NOT EXISTS shellies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            device TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
        cursor = db_conn.cursor()
        cursor.execute(sql)
        db_conn.commit()
        cursor.close()

        try:
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            mqtt_client.on_connect = on_connect
        except AttributeError:
            mqtt_client = mqtt.Client()
            mqtt_client.on_connect = on_connect_v1

        mqtt_client.on_message = on_message
        mqtt_client.user_data_set({"db_conn": db_conn})

        mqtt_client.connect(host=mqtt_host, port=mqtt_port, keepalive=60)
        print(f"II: Connected to {mqtt_host}:{mqtt_port}")
        try:
            mqtt_client.loop_forever()
        except KeyboardInterrupt:
            pass
        mqtt_client.disconnect()
        print(f"II: Disconnected from {mqtt_host}:{mqtt_port}")
    print(f"II: Disconnected from {database_file}")


if __name__ == "__main__":
    main()
