import os
import json
import logging

from dataclasses import dataclass
from datetime import date, datetime
from random import randint
from time import sleep
from kafka import KafkaProducer, errors


@dataclass
class Config:
    capture_output: bool
    sensor_id: str
    topic_name: str
    env: str = os.environ("env") | "dev"
    output_file: str = False
    produce_throttle_rate: int = 0.1


@dataclass
class SensorData:
    timestamp: date
    values: list[int]


conf = Config(capture_output=False, output_file="sensor1.json",
              sensor_id="sensor1", topic_name="json.sensor1")

logger = logging.getLogger(f"{conf.sensor_id} Producer")


def gen_timestamps() -> list:
    """
    Generates a list of timestamps for every minute of the current date.

    Returns:
        list: Unix epoch formatted timestamps for every minute of the current date.
    """
    month = datetime.now().month
    day = datetime.now().day
    year = datetime.now().year
    date_str = str(month) + "-" + str(day) + "-" + str(year)

    timestamps = []
    for hour in range(24):
        for minute in range(60):
            timestamp_str = date_str + " " + \
                str(hour).zfill(2) + ":" + str(minute).zfill(2) + \
                ":00." + str(randint(100, 900))

            timestamp_unix = int(datetime.strptime(
                timestamp_str, "%m-%d-%Y %H:%M:%S.%f").timestamp() * 1000)

            timestamps.append(timestamp_unix)
    return timestamps


def generate_value_arrays() -> list[int]:
    """
    Generates a list of value arrays for every minute of the current date.

    Returns:
        list: Value arrays for every minute of the current date.
    """
    
    return [randint(0, 100) for _ in range(60)]


def gen_sensor_data() -> list[SensorData]:
    """_summary_

    Returns:
        list[RawSensorData]: Generates the sensor data.
    """
    timestamps = gen_timestamps()
    return [SensorData(timestamp, values=generate_value_arrays()) for timestamp in timestamps]

def write_output(data: list[SensorData]) -> None:
    with open(conf.output_file, "w") as f:
        f.write(json.dumps([ob.__dict__ for ob in data], indent=4))
        
def produce_data() -> None:
    """
    Produces data to Kafka.
    """
    try:
        producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        records = gen_sensor_data()

        if conf.capture_output:
            write_output()

        for record in records:
            key = conf.sensor_id.encode("utf-8")
            message = {"timestamp": record.timestamp, "values": record.values}

            if conf.env == "prod":
                producer.send(conf.topic_name, key=key, value=message)
                producer.flush()
            else:
                logger.info(f"{key}: {message}")
            
            sleep(conf.produce_throttle_rate)

    except errors.NoBrokersAvailable:
        logger.error("Broker not available. Please check if Kafka is running.")


if __name__ == "__main__":
    produce_data()
