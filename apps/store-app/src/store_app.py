import json
import time
import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from confluent_kafka import Consumer, KafkaError

from config import Config


def consume_and_save_parquet(
    kafka_broker,
    consumer_topic,
    group_id,
    batch_size,
    timeout,
    s3_endpoint_url,
    s3_access_key_id,
    s3_secret_access_key,
    s3_bucket_name,
):
    """
    Consumes messages from a Kafka topic, aggregates them, and saves to Parquet files.
    """
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": kafka_broker,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                # 'enable.auto.commit': False
            }
        )
        consumer.subscribe([consumer_topic])

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
        )

        message_buffer = []
        last_write_time = time.time()
        print(f"Listening for messages on topic: {consumer_topic}")
        while True:
            # Poll for new messages with a short timeout
            messages = consumer.poll(timeout=1.0)

            if messages is None:
                continue
            elif messages.error():
                if messages.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro: {messages.error()}")
            else:
                message_buffer.append(json.loads(messages.value().decode("utf-8")))
                # Check if the buffer is full or a timeout has occurred
                if len(message_buffer) >= batch_size:
                    parquet_table = to_parquet(message_buffer)
                    file_name = f"census-data-{int(time.time() * 1000)}.parquet"
                    upload_to_s3(s3_client, parquet_table, s3_bucket_name, file_name)
                    message_buffer.clear()
                    last_write_time = time.time()

                elif message_buffer and (time.time() - last_write_time > timeout):
                    print(
                        f"Timeout reached. Writing remaining {len(message_buffer)} messages."
                    )
                    parquet_table = to_parquet(message_buffer)
                    file_name = f"census-data-{int(time.time() * 1000)}.parquet"
                    upload_to_s3(s3_client, parquet_table, s3_bucket_name, file_name)
                    message_buffer.clear()
                    last_write_time = time.time()

    except KeyboardInterrupt:
        print("Shutting down Store App")
    finally:
        # Write any remaining messages before exiting
        if message_buffer:
            parquet_table = to_parquet(message_buffer)
            file_name = f"census-data-{int(time.time() * 1000)}.parquet"
            upload_to_s3(s3_client, parquet_table, s3_bucket_name, file_name)
            message_buffer.clear()

        consumer.close()
        s3_client.close()
        print("Store App closed.")


def upload_to_s3(s3_client, parquet_table, bucket_name, object_name):
    """
    Uploads a file to an S3 bucket.
    """
    try:
        parquet_buffer = io.BytesIO()
        pq.write_table(parquet_table, parquet_buffer)
        s3_client.upload_fileobj(parquet_buffer, bucket_name, object_name)
        print(f"Uploaded {object_name} to s3://{bucket_name}/{object_name}")

    except Exception as e:
        print(f"Error uploading to S3: {e}")


def to_parquet(messages):
    """
    Converts a list of JSON messages to a DataFrame and saves it as a Parquet file.
    """
    if not messages:
        return

    try:
        df = pd.DataFrame(messages)
        df["VAL_COMP_ELEM4"] = pd.to_numeric(df["VAL_COMP_ELEM4"], errors="coerce")
        table = pa.Table.from_pandas(df)
        return table

    except Exception as e:
        print(f"An error occurred while writing to Parquet: {e}")


def main():
    config = Config()
    consume_and_save_parquet(
        config.KAFKA_BROKER,
        config.CENSUS_DATA_TOPIC,
        config.GROUP_ID,
        config.BATCH_SIZE,
        config.BATCH_TIMEOUT_SECONDS,
        config.S3_ENDPOINT_URL,
        config.S3_ACCESS_KEY_ID,
        config.S3_SECRET_ACCESS_KEY,
        config.S3_BUCKET_NAME,
    )


if __name__ == "__main__":
    main()
