**X API Handler and Kafka Producer Client Application**
X API Handler
The X API handler is a Python class designed to interact with the Twitter API using bearer token authentication. It provides methods to manage rules for filtering tweets, retrieve tweets from the streaming endpoint, and process the tweet data.

Prerequisites
Python 3.x
Required Python packages: requests, pandas
Usage
Instantiate the XApiHandler class with a valid bearer token.

python
Copy code
x_api = XApiHandler(BEARER_TOKEN)
Use the provided methods to interact with the Twitter API:

get_rules(): Retrieves the current filtering rules.
delete_all_rules(rules): Deletes all existing filtering rules.
set_rules(delete): Sets new filtering rules.
get_stream(): Retrieves tweets from the streaming endpoint.
Kafka Producer Client Application
The Kafka producer client application consumes tweet data received from the X API handler and produces it into a Kafka topic. It uses the confluent_kafka library for Kafka integration.

Prerequisites
Kafka broker(s) accessible from the client application.
Required Python packages: confluent_kafka, json
Configuration
Ensure that the Kafka broker(s) and topic(s) are properly configured in the config.py file.

python
Copy code
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'tweets-topic'
Usage
Run the Kafka producer client application with the appropriate configuration.
bash
Copy code
python kafka_producer.py
This README.md file provides an overview of the X API handler and the Kafka producer client application, including their usage, prerequisites, and configuration. Make sure to follow the instructions provided to set up and run the applications successfully.