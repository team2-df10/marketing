from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep

def load_avro_schema_from_file():
    key_schema = avro.load("/home/azril/marketing/kafka/marketing_data_key.avsc")
    value_schema = avro.load("/home/azril/marketing/kafka/marketing_data_value.avsc")

    return key_schema, value_schema

def send_record():
    key_schema, value_schema = load_avro_schema_from_file()
    
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    file = open('/home/azril/marketing/kafka/raw_bank_marketing.csv')
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
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
                 "nr_employed": int(row[20]),
                 "subcribed": row[21]
                 }

            try:
                producer.produce(topic='marketing_data', key=key, value=value)
                print(f"Successfully producing record value - {value}")
            except Exception as e:
                print(f"Execption while producing record value - {value}: {e}")

            producer.flush()
            sleep(1)

if __name__ == "__main__":
    send_record()