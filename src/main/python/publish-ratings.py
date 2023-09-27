#!/bin/python

import argparse
import numpy as np
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from confluent_kafka.avro import AvroProducer


def parse_args():
    parser = argparse.ArgumentParser(description='Publish CSV records to a Kafka topic.')
    parser.add_argument('--delta-minutes', '-d', default=2, type=int, help='Minutes to shift timestamps')
    parser.add_argument('--input-file', '-i', default='../resources/ml-latest-small/ratings.csv',
                        help='Path to the CSV input file')
    parser.add_argument('--output-topic', '-o', default='user-ratings-4', help='Output Kafka topic')
    return parser.parse_args()


def transform_timestamps(df, start_time_adjusted, end_time_adjusted):
    start_time_adjusted_ts = int(start_time_adjusted.timestamp() * 1000)
    end_time_adjusted_ts = int(end_time_adjusted.timestamp() * 1000)

    # Generate random timestamps within the time range
    random_timestamps = np.random.randint(start_time_adjusted_ts, end_time_adjusted_ts, df.shape[0])

    # Sort the random timestamps
    sorted_timestamps = np.sort(random_timestamps)

    # Assign sorted random timestamps to the DataFrame
    df['timestamp'] = sorted_timestamps

    return df


def setup_producer():
    # Avro schema
    # key schema not used due to streams dependency confusing key and value schemas
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
    total_count = df.shape[0]
    record_count = 0
    while not df.empty:
        current_utc_ts = int(datetime.utcnow().timestamp() * 1000)  # Convert this to UNIX milliseconds as well
        past_rows = df[df['timestamp'] <= current_utc_ts]

        if not past_rows.empty:
            batch = past_rows.to_dict('records')
            send_batch(producer, batch, output_topic)

            batch_count = len(batch)
            record_count += batch_count

            current_time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{current_time_str} - {batch_count} rows sent ({record_count}/{total_count})")

            df = df[df['timestamp'] > current_utc_ts]
            producer.flush()

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
