# Credit Card Fraud Pipeline Subscription End-to-End Data Pipeline

## Bussiness Understanding

A banking institution conducted campaigns to promote term deposits to its clients, primarily using telemarketing methods like direct phone calls. The target variable (y) indicates whether the client agreed ('yes') or declined ('no') to place a deposit after the campaign. 

The bank aims to identify the features or client statuses that lead to successful offers, making their campaigns more cost and time efficient.

## Problem Statements

The bank seeks to increase the campaign's efficiency by targeting clients with a higher chance of accepting the deposit offer, using the features from the data.

## Goal
To understand the target audience for the campaign, a data pipeline is created to facilitate data analysis and reporting application record.

## Objective
The objectives of this projects are described below:

-Design an end-to-end data pipeline with Lambda Architecture, providing the business intelligence/analyst team with the flexibility to choose between using batched data or real-time streamed data. This ensures efficient data processing and empowers timely decision-making.

-Create an analytics dashboard to derive meaningful insights and assist the business intelligence/analyst team in making data-driven decisions.

## Data Pipeline
![image](https://github.com/team2-df10/marketing/assets/122470555/0664d9c7-3279-4225-8e29-9c8f037a61cc)



## Tools

- Cloud : Google Cloud Platform
- Infrastructure as Code : Terraform
- Containerization : Docker, Docker Compose
- Stream Processing : Kafka
- Orchestration: Airflow
- Transformation : Spark, dbt
- Data Lake: Google Cloud Storage
- Data Warehouse: BigQuery
- Data Visualization: Looker
- Language : Python

## Dataset
![image](https://github.com/team2-df10/marketing/assets/122470555/72fdc329-4d5d-4322-ad1f-159878467b73)
![image](https://github.com/team2-df10/marketing/assets/122470555/f84f8718-a859-4c33-86f3-555579583ffa)



## Data Visualization Dashboard
![Screenshot (188)](https://user-images.githubusercontent.com/108534539/230117610-c579e654-8bf5-487b-be4f-f0354212f220.png)

![Screenshot (187)](https://user-images.githubusercontent.com/108534539/230117643-9577559c-ac6d-4e47-8dcf-4af817646479.png)


## Google Cloud Usage Billing Report
Data infrastructure we used in this project are entirely built on Google Cloud Platform with more or less 3 weeks of project duration, 
using this following services:
- Google Cloud Storage (pay for what you use)
- Google BigQuery (first terrabyte processed are free of charge)
- Google Looker Studio (cost is based from number of Looker Blocks (data models and visualizations), users, and the number of queries processed per month)
> Total cost around 6$ out of 300$ free credits that GCP provided

## Project Instruction
### Clone this repository and enter the directory
```bash
git clone https://github.com/team2-df10/marketing
```


### Create a file named "service-account.json" containing your Google service account credentials and copy file to dbt folder
```json
{
  "type": "service_account",
  "project_id": "[PROJECT_ID]",
  "private_key_id": "[KEY_ID]",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[PRIVATE_KEY]\n-----END PRIVATE KEY-----\n",
  "client_email": "[SERVICE_ACCOUNT_EMAIL]",
  "client_id": "[CLIENT_ID]",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[SERVICE_ACCOUNT_EMAIL]"
}
```
### Cloud Resource Provisioning with Terraform

1. Install `gcloud` SDK, `terraform` CLI, and create a GCP project. Then, create a service account with **Storage Admin**, **Storage Pbject Admin**, and **BigQuery Admin** role. Download the JSON credential and store it on `service-account.json`. 

2. Enable IAM API and IAM Credential API in GCP.

3. Change directory to `terraform` by executing
```
cd terraform
```
4. Open `terraform/main.tf` in a text editor, and fill your GCP's project id.

5. Initialize Terraform (set up environment and install Google provider)
```
terraform init
```
6. In terminal you will be asked for a value to continue progress so you just need to write your GCP Project ID then click enter.

7. Plan Terraform infrastructure creation
```
terraform plan
```
8. Create new infrastructure by applying Terraform plan
```
terraform apply
```
  You will get this message if it is working properly
  ![image](https://github.com/team2-df10/marketing/assets/122470555/b99230d3-e516-4693-befd-21a468998d0c)



9. Check GCP console to see newly-created resources.

### Batch Pipeline

1. Setting dbt in profiles.yml

2. Create batch pipeline with Docker Compose
```bash
sudo docker-compose up
```
3. Open Airflow with username and password "airflow" to run the DAG
```
localhost:8090
```

![image](https://user-images.githubusercontent.com/108534539/230137434-ca2e097f-2003-4cf1-8578-a1bc69c0f73d.png)

![image](https://user-images.githubusercontent.com/108534539/231919353-46fb7526-6c9e-4bce-a2f1-752ef3c02012.png)


4. Open Spark to monitor Spark master and Spark workers
```
localhost:8080
```
![image](https://user-images.githubusercontent.com/108534539/230136347-1fe5de5e-3585-4b04-8665-a14512f0efe3.png)


### Streaming Pipeline

1. Enter directory kafka
```bash
cd kafka
```

2. Create streaming pipeline with Docker Compose
```bash
sudo docker-compose up
```

3. Install required Python packages
```bash
pip install -r requirements.txt
```

4. Run the producer to stream the data into the Kafka topic
```bash
python3 producer.py
```

5. Run the consumer to consume the data from Kafka topic and load them into BigQuery
```bash
python3 consumer.py
```

![image](https://github.com/team2-df10/marketing/assets/122470555/dfbc8310-78db-449d-be86-c66a2e871585)


6. Open Confluent to view the topic
```
localhost:9021
```
![image](https://user-images.githubusercontent.com/108534539/230141014-bb9ef28b-af25-4fa8-b49a-ce5ef8f69aa2.png)

7. Open Schema Registry to view the active schemas
```
localhost:8081/schemas
```
![image](https://user-images.githubusercontent.com/108534539/230141266-c959f01b-b51e-4dc4-8adf-39cd820f466a.png)
