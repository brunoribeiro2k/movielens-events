#!/bin/python

import argparse
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from confluent_kafka.avro import AvroProducer

def parse_args():
    parser = argparse.ArgumentParser(description='Publish CSV records to a Kafka topic.')
    parser.add_argument('--delta-minutes', '-d', default=2, type=int, help='Minutes to shift timestamps')
    parser.add_argument('--input-file', '-i', default='ml-latest-small/ratings.csv', help='Path to the CSV input file')
    parser.add_argument('--output-topic', '-o', default='user-ratings-4', help='Output Kafka topic')
    return parser.parse_args()

def transform_timestamps(df, start_time_adjusted, end_time_adjusted):
    start_time_adjusted_ts = start_time_adjusted.timestamp()
    end_time_adjusted_ts = end_time_adjusted.timestamp()
    delta_adjusted_ts = end_time_adjusted_ts - start_time_adjusted_ts

    start_time_data_ts = df['timestamp'].min()
    end_time_data_ts = df['timestamp'].max()
    delta_data_ts = end_time_data_ts - start_time_data_ts

    df['timestamp'] = ((df['timestamp'] - start_time_data_ts) / delta_data_ts * delta_adjusted_ts + start_time_adjusted_ts) * 1000
    df['timestamp'] = df['timestamp'].astype(int)
    return df


def setup_producer():
    # Avro schema
    key_schema_str = json.dumps({
        "namespace": "ratings",
        "type": "record",
        "name": "UserID",
        "fields": [
            {"name": "userId", "type": "int"}
        ]
    })
    value_schema_str = json.dumps({
        "namespace": "ratings",
        "type": "record",
        "name": "Rating",
        "fields": [
            {"name": "userId", "type": "int"},
            {"name": "movieId", "type": "int"},
            {"name": "rating", "type": "float"},
            {
                "name": "timestamp",
                "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                }
            }
        ]
    })

    # Producer configuration
    producer = AvroProducer({
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081'
    }, default_value_schema=value_schema_str)
    # }, default_value_schema=value_schema_str, default_key_schema=key_schema_str)

    return producer

def send_batch(producer, batch, output_topic):
    for value in batch:
        producer.produce(
            topic=output_topic,
            # key=dict(userId=value['userId']),
            value=value
        )

def manage_loop(df, producer, output_topic):
    record_count = 0
    while not df.empty:
        current_utc_ts = int(datetime.utcnow().timestamp() * 1000)  # Convert this to UNIX milliseconds as well
        past_rows = df[df['timestamp'] <= current_utc_ts]

        if not past_rows.empty:
            batch = past_rows.to_dict('records')
            send_batch(producer, batch, output_topic)
            record_count += len(batch)
            
            df = df[df['timestamp'] > current_utc_ts]
            producer.flush()
            print(f"Sent {record_count} records.")

        time.sleep(1)


def main():
    args = parse_args()
    
    df = pd.read_csv(args.input_file)
    start_time_adjusted = datetime.utcnow()
    end_time_adjusted = start_time_adjusted + timedelta(minutes=args.delta_minutes)

    df = transform_timestamps(df, start_time_adjusted, end_time_adjusted)
    producer = setup_producer()
    
    print(f"Started publishing records at {start_time_adjusted}")
    manage_loop(df, producer, args.output_topic)
    print(f"Finished publishing records at {datetime.utcnow()}")

if __name__ == "__main__":
    main()
