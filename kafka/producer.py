#import libraries
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep

class Producer:
    """
    A Kafka producer that sends marketing data from a CSV file to a Kafka topic
    using Avro schemas.
    """
    def __init__(self,config, data_path, schema_path):
        """
        Initialize with Kafka producer configuration, Avro schema path, and CSV data path.
        Load the Avro schema and create a Kafka AvroProducer instance.
        """
        self.config = config
        self.data_path = data_path
        self.schema_path = schema_path
        self.key_schema, self.value_schema = self.load_avro_schema()
        self.producer = AvroProducer(self.config, default_key_schema=self.key_schema, default_value_schema=self.value_schema)
    

    def load_avro_schema(self):
        """
        Load Avro key and value schemas from files.
        """
        key_schema = avro.load(f"{self.schema_path}/marketing_data_key.avsc")
        value_schema = avro.load(f"{self.schema_path}/marketing_data_value.avsc")
        return key_schema, value_schema

    def send_record(self):
        """
        Send records from the CSV file to the Kafka topic.
        Each record is parsed and sent individually.
        """
        with open(self.data_path,'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                key,value = self.parse_row(row)
                try:
                    self.producer.produce(topic='marketing_data', key=key, value=value)
                    print(f"Successfully producing record value - {value}")
                except Exception as e:
                    print(f"Execption while producing record value - {value}: {e}")
            
            self.producer.flush()
            sleep(1)

    def parse_row(self, row):
        """
        Parse a row of the CSV file into key-value pairs.
        """
        key = {"client_id": int(row[0])}
        value = {
                 "client_id": int(row[0]),
                 "age": int(row[1]),
                 "job": row[2],
                 "marital": row[3],
                 "education": row[4],
                 "credit": row[5],
                 "housing": row[6],
                 "loan": row[7],
                 "contact": row[8],
                 "month": row[9],
                 "day_of_week": row[10],
                 "duration": int(row[11]),
                 "campaign": int(row[12]),
                 "pdays": int(row[13]),
                 "previous": int(row[14]),
                 "poutcome": row[15],
                 "emp_var_rate": float(row[16]),
                 "cons_price_idx": float(row[17]),
                 "cons_conf_idx": float(row[18]),
                 "euribor3m": float(row[19]),
                 "nr_employed": float(row[20]),
                 "subscribed": row[21]
                 }
        return key, value

if __name__ == "__main__":
    config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1",
        "auto.offset.reset": "earliest"}
    schema_path = "/home/azril/marketing/kafka/"
    data_path = "/home/azril/marketing/kafka/raw_bank_marketing.csv"
    producer = Producer(config, data_path, schema_path)
    producer.send_record()