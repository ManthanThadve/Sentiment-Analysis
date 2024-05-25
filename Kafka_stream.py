# This is a simple example of the SerializingProducer using Avro.
#
# import argparse
import configparser
from uuid import uuid4
import pandas as pd
import os

from X_filtered_stream import XApiHandler

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")


class Tweet:
    def __init__(self, author_id: str, created_at: str, id: str, edit_history_tweet_ids: list[str], text: str):
        self.author_id = author_id
        self.created_at = created_at
        self.id = id
        self.edit_history_tweet_ids = edit_history_tweet_ids
        self.text = text

    def __repr__(self):
        return f"Tweet(author_id={self.author_id}, created_at={self.created_at}, id={self.id}, edit_history_tweet_ids={self.edit_history_tweet_ids}, text={self.text})"



def record_to_dict(tweet, ctx):
    """
    Returns a dict representation of a Tweet instance for serialization.

    Args:
        tweet (Tweet): Tweet instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.

    """
    if tweet is None:
        return None

    # User._address must not be serialized; omit from dict
    return dict(
                 author_id = tweet.author_id,
                 created_at = tweet.created_at,
                 id = tweet.id,
                 edit_history_tweet_ids = tweet.edit_history_tweet_ids,
                 text = tweet.text )


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    """
    if err is not None:
        print("Delivery failed for Tweet record {}: {}".format(msg.key(), err))
        return
    print('Twitter event {} successfully produced to topic :{} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args['topic_name']

    schema_file_path = args['avro_schema_file_path']
    with open(schema_file_path, 'r') as schema_file:
        schema_str = schema_file.read()
        
    # print(schema_str)
    
    sr_conf = {'url': args['schema_registry']}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    string_serializer = StringSerializer('utf-8')
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     record_to_dict)

    producer_conf = {'bootstrap.servers': args['bootstrap_servers'],
                     'key.serializer': string_serializer,
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing tweet records to topic {}. ^C to exit.".format(topic))
        # Serve on_delivery callbacks from previous calls to produce()
    
    print("getting Dataframe from X API with 100 tweets data")
    x_api = XApiHandler(BEARER_TOKEN)
    df = x_api.get_X_data_as_df()

    for _, row in df.iterrows():
        producer.poll(20.0)
        try:

            # Produce records to the Kafka topic
            
            record = {
                 "author_id": row['author_id'] if pd.notna(row['author_id']) else None,
                 "created_at": row['created_at'] if pd.notna(row['created_at']) else None,
                 "id": row['id'] if pd.notna(row['id']) else None,
                 "edit_history_tweet_ids": row['edit_history_tweet_ids'] if pd.notna(row['edit_history_tweet_ids']) else None,
                 "text": row['text'] if pd.notna(row['text']) else None,
            }

            record_obj = Tweet(auther_id = record['auther_id'],
                                created_at = record['created_at'],
                                id = record['id'],
                                edit_history_tweet_ids = record['edit_history_tweet_ids'],
                                text = record['text'],
                              )
            
            producer.produce(topic=topic, key=str(uuid4()), value=record_obj,
                            on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()

if __name__ == '__main__':

    parser = configparser.ConfigParser()
    parser.read("M:\\Training\\Kafka\\Kafka-Producer-Consumer\\credentials.conf")

# All the required configuration information is to be found inside config dictionary

    config = {'bootstrap_servers': "localhost:9092",
              'schema_registry': "http://localhost:8081",
              'topic_name': "twitter_posts",
              'avro_schema_file_path': "M:\\put\\your\\Schema file path\\tweets.avsc"
                }

    main(config)