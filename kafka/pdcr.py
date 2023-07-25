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
    def __init__(self,config, data_path):
        """
        Initialize with Kafka producer configuration, Avro schema path, and CSV data path.
        Load the Avro schema and create a Kafka AvroProducer instance.
        """
        self.config = config
        self.data_path = data_path
        self.key_schema, self.value_schema = self.load_avro_schema()
        self.producer = AvroProducer(self.config, default_key_schema=self.key_schema, default_value_schema=self.value_schema)
        self.csv_file = open(self.data_path)

    def load_avro_schema(self):
        """
        Load Avro key and value schemas from files.
        """
        #return avro.load(f"{self.schema_path}/marketing_data_key.avsc"), avro.load(f"{self.schema_path}/marketing_data_value.avsc")
        key_schema = avro.load(self.schema_path + "marketing_data_key.avsc")
        value_schema = avro.load(self.schema_path + "marketing_data_value.avsc")
        return key_schema, value_schema

    def send_record(self):
        """
        Send records from the CSV file to the Kafka topic.
        Each record is parsed and sent individually.
        """
        csvreader = csv.reader(self.csv_file)
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
        try:
            emp_var_rate = float(row[17])
            cons_price_idx = float(row[18].replace(' ', '.'))
            euribor3m = float(row[20].replace(' ', '.'))
            cons_conf_idx = float(row[19])
        except ValueError as e:
            print(f"Failed to convert float field: {e}")
                
        key = {"ID": int(row[0])}
        value = {
            "ID": int(row[0]),
            "Date": str(row[1]),
            "Age": int(row[2]),
            "Job": str(row[3]),
            "Marital": str(row[4]),
            "Education": str(row[5]),
            "Default": str(row[6]),
            "Housing": str(row[7]),
            "Loan": str(row[8]),
            "Contact": str(row[9]),
            "Month": str(row[10]),
            "Day_of_week": str(row[11]),
            "Duration": int(row[12]),
            "Campaign": int(row[13]),
            "pdays": int(row[14]),
            "Previous": int(row[15]),
            "Poutcome": str(row[16]),
            "Emp_var_rate": emp_var_rate,
            "Cons_conf_idx": cons_conf_idx,
            "Cons_price_idx": cons_price_idx,
            "Euribor3m": euribor3m,
            "Nr_employed": int(row[21]),
            "y": str(row[22])
        }
        return key, value

if __name__ == "__main__":
    config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1",
        "auto.offset.reset": "earliest"}
    data_path = "/home/azril/marketing/kafka/raw_bank_marketing.csv"
    producer = Producer(config , data_path)
    producer.send_record()