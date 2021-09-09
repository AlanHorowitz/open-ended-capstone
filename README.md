##  Widgets Unlimited - A data warehousing simulation

### Background:   
Widgets Unlimited is a company that sells widgets via an e-commerce website and in physical retail stores.   They have three operational systems that support their business: Inventory, e-commerce, and in-store.
### Problem: 
The company needs accurate and timely sales information captured by these operational systems to support marketing and pricing decisions.  The systems have some limited reporting capabilities, but they do not consolidate sales across e-commerce and physical stores or support ad-hoc queries. It is determined that changes to the systems may be captured as follows: Inventory via csv file (daily); e-commerce via kafka topic (continuously); and in-store via postgres database (daily).
### Proposal: 
A data warehouse, which will extract data from the operational systems (source systems to the warehouse) and transform it into an analysis-friendly star schema format.
### Source Systems and Data:
This project includes a data generation component.  Datasets for commercial operational data may not be freely available and synthesized data is convenient for demonstrating cleaning and harmonization use cases.  The source system classes react to this data and expose in different formats as incremental updates  An operations simulator ties these all together by repeatedly populating the source systems which in turn produce the inputs for the data warehouse.
### Goals: 
A successful implementation of the data warehouse will ingest incremental initial and incremental loads, clean data errors, and harmonize the differences in the operational systems. In particular, date and location information will be standardized and the sales facts from the e-commerce and physical stores will be combined into a single fact table.  An orchestration layer will coordinate ingestion for the three source systems and trigger the transformations which update the star schema.
### Tools:
 - Languages: python, sql, pandas
 - Databases: PostGres, mySQL
 - Messaging: Kafka
 - Containerization:  Docker
 - Orchestration: Airflow
###  Cloud Migration:  The application will be migrated to the Azure cloud to take advantage of hosted database and kafka cluster services.   As data volumes increase, a migration to Azure Synapse would be considered.

### Architecture

![overview](./images/overview.png)

### Project structure
### Use Cases
    - Database 
    - CSV
    - Kafka Stream
    - Data normalization and deduplication
### Phased Rollout
    1. Data Generator and Customer Dimension
    2. Other Dimensions and Facts
    3. Operation Systems and Ingestions (synchronous one container)
    4. Synchronous separate containers
    5. Cloud Deployment
    6. Asynchronous Orchestration

### Install and run the project



### Prerequisites: The simulation will run in any environment with docker, docker-compose and python venv.  The development environment was:

 - ubuntu 20.04 
 - python 3.8.10
 - python3.8-venv  
 - docker 2.10.8
 - docker-compose 1.25.5

### Installation steps

1. python3 -m venv ~/virtualenvs/open-ended-capstone
1. source ~/virtualenvs/open-ended-capstone/bin/activate
1. git clone https://github.com/AlanHorowitz/open-ended-capstone.git
1. cd open-ended-capstone
1. docker network create widgets-unlimited-network
1. docker network inspect widgets-unlimited-network | grep Gateway
1. if gateway host shown is not 172.18.0.1, edit config.sh to change the host to the shown ip address
1. source config.sh
1. docker-compose -f docker-compose-db.yaml up -d
1. pip install -r requirements-sourcesystems.txt 
   
### To run demo1.py

1. cd ~/open-ended-capstone/WidgetsUnlimited
1. python3 demo1.py

### To run tests

1. cd ~/open-ended-capstone/tests
1. pytest

### To run demo1.py in a docker container

1. cd ~/open-ended-capstone
1. docker-compose build
1. docker-compose up

