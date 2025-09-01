import sys
import json

import boto3
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError

from config import Config


def produce_to_kafka(
    kafka_broker,
    request_topic,
    group_id,
    census_data_topic,
    s3_endpoint_url,
    s3_access_key_id,
    s3_secret_access_key,
):
    """
    Reads a CSV file, converts each row to a JSON object, and sends it to a Kafka topic.
    """

    try:
        # Initialize the Kafka producer
        consumer = Consumer(
            {
                "bootstrap.servers": kafka_broker,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                # 'enable.auto.commit': False
            }
        )
        consumer.subscribe([request_topic])
        producer = Producer({"bootstrap.servers": kafka_broker})
        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
        )

        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            elif message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Error: {message.error()}")
                    continue
            else:
                request = json.loads(message.value().decode("utf-8"))
                # Check if request dict has bucket and file
                if not all(
                    key in request.keys() for key in ["bucket-name", "file-key"]
                ):
                    print(
                        "❌ Error: Invalid request, missing 'bucket-name' or 'file-key'"
                    )
                    continue

                # Get object directly from S3
                obj = s3_client.get_object(
                    Bucket=request["bucket-name"], Key=request["file-key"]
                )

                # obj['Body'] is a StreamingBody, which behaves like a file object
                df = pd.read_csv(obj["Body"], sep=";")

                # Convert the DataFrame to a list of dictionaries. Each dictionary represents a row.
                records = df.to_dict(orient="records")

                success_count = 0
                for i, record in enumerate(records):
                    try:
                        msg = json.dumps(record).encode("utf-8")
                        producer.produce(census_data_topic, msg)
                        producer.poll(0)  # Dispara entrega assíncrona
                        success_count += 1

                    except KafkaError as e:
                        print(f"Error sending record {i+1}: {e}", file=sys.stderr)

                # Flush all messages to the broker
                producer.flush()

                print("-" * 40)
                print(
                    f"Successfully read {request['file-key']} and sent {success_count} of {len(records)} messages."
                )

    except KeyboardInterrupt:
        print("Stopping Load App...")
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        consumer.close()
        s3_client.close()
        print("Load App closed.")


def main():
    config = Config()

    produce_to_kafka(
        config.KAFKA_BROKER,
        config.REQUEST_TOPIC,
        config.GROUP_ID,
        config.CENSUS_DATA_TOPIC,
        config.S3_ENDPOINT_URL,
        config.S3_ACCESS_KEY_ID,
        config.S3_SECRET_ACCESS_KEY,
    )


if __name__ == "__main__":
    main()
