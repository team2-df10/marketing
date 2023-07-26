from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os
from confluent_kafka import KafkaError

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/azril/marketing/kafka/service-account.json"

class Consumer:
    def __init__(self, config, dataset_name, table_name):
        """
        Initialize with Kafka consumer configuration, and BigQuery dataset and table names.
        Subscribe the consumer to the Kafka topic and setup the BigQuery table.
        """
        self.config = config
        self.consumer = AvroConsumer(self.config)
        self.consumer.subscribe(["marketing_data"])
        self.client = bigquery.Client()
        self.table = self.setup_bigquery_table(dataset_name, table_name)
        
    def setup_bigquery_table(self, dataset_name, table_name):
        """
        Retrieves the existing BigQuery table based on the provided dataset and table name.
        """
        schema = [
            bigquery.SchemaField('client_id', 'INT64'),
            bigquery.SchemaField('age', 'INT64'),
            bigquery.SchemaField('job', 'STRING'),
            bigquery.SchemaField('marital', 'STRING'),
            bigquery.SchemaField('education', 'STRING'),
            bigquery.SchemaField('credit', 'STRING'),
            bigquery.SchemaField('housing', 'STRING'),
            bigquery.SchemaField('loan', 'STRING'),
            bigquery.SchemaField('contact', 'STRING'),
            bigquery.SchemaField('month', 'STRING'),
            bigquery.SchemaField('day_of_week', 'STRING'),
            bigquery.SchemaField('duration', 'INT64'),
            bigquery.SchemaField('campaign', 'INT64'),
            bigquery.SchemaField('pdays', 'INT64'),
            bigquery.SchemaField('previous', 'INT64'),
            bigquery.SchemaField('poutcome', 'STRING'),
            bigquery.SchemaField('cons_conf_idx', 'FLOAT64'),
            bigquery.SchemaField('emp_var_rate', 'FLOAT64'),
            bigquery.SchemaField('cons_price_idx', 'FLOAT64'),
            bigquery.SchemaField('euribor3m', 'FLOAT64'),
            bigquery.SchemaField('nr_employed', 'INT64'),
            bigquery.SchemaField('subscribed', 'STRING')
        ]

        dataset_ref = self.client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)
        try:
            table = self.client.get_table(table_ref)  # Make an API request.
            print(f"Table {table_name} already exists.")
        except NotFound:
            print(f"Table {table_name} is not found. Creating a new one.")
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)  # Make an API request.
            print(f"Created table {table_name}.")
        
        return table

    def read_messages(self):
        """
        Continuously read messages from the Kafka topic and insert them into the BigQuery table.
        """
        while True:
            try:
                # Poll the message from Kafka
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {message.topic()}/{message.partition()}")
                    else:
                        print(f"Consumer error: {message.error()}")
                    continue
                
                print(f"Received record from Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                
                row_to_insert = {field.name: message.value().get(field.name) for field in self.table.schema}
                errors = self.client.insert_rows(self.table, [row_to_insert])
                if errors:
                    print(f"Errors occurred while inserting rows: {errors}")
                else:
                    self.consumer.commit()

            except Exception as e:
                print(f"Exception while inserting into BigQuery: {e}")
                continue

        self.consumer.close()

if __name__ == "__main__":
    config = {"bootstrap.servers": "localhost:9092",
              "schema.registry.url": "http://localhost:8081",
              "group.id": "marketingData.avro.consumer.1",
              "auto.offset.reset": "earliest"} # Fill in your Kafka configuration details here
    # Fill in your BigQuery dataset and table names here
    dataset_name = 'final_project'
    table_name = 'marketing_data_table_kafka'
    consumer = Consumer(config, dataset_name, table_name)
    consumer.read_messages()
