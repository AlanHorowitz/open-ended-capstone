##  Widgets Unlimited - A data warehousing simulation

### Background:   
Widgets Unlimited is a company that sells widgets via an e-commerce website and in physical retail stores.   They have three operational systems that support the business: Inventory, e-commerce, and in-store.
### Problem: 
The company needs accurate and timely sales information captured by these operational systems to support marketing and pricing decisions.  The systems have some limited reporting capabilities, but they do not consolidate sales across e-commerce and physical stores or support ad-hoc queries. It is determined that changes to the systems may be captured as follows: Inventory via csv file (daily); e-commerce via kafka topic (continuously); and in-store via postgres database (daily).
### Proposal: 
Build a data warehouse, which will extract data from the operational systems (source systems from the point of view of the warehouse) and transform it into an analysis-friendly star schema format.
### Operational Systems and Data:
This project includes a data generation component.  Datasets of commercial operational data may not be freely available and synthesized data is convenient for demonstrating cleaning and harmonization use cases.  Operational system classes react to this data and expose it as incremental updates (to be consumed by the data warehouse) in different formats.  An operations' simulator ties this together by repeatedly calling the data generator and passing the new or modified records to the operational systems.
### Goals: 
A successful implementation of the data warehouse will ingest initial and incremental loads, clean data errors, and harmonize the differences in the operational systems. In particular, date and location information will be standardized and the sales facts from the e-commerce and physical stores will be combined into a single fact table.  An orchestration layer will coordinate ingestion for the three source systems and trigger the transformations which update the star schema.
### Tools:
 - Languages: python, sql, pandas
 - Databases: Postgres, mySQL
 - Messaging: Kafka
 - Containerization:  Docker
 - Orchestration: Airflow

![overview](./images/overview.png)
<p align="center">Widgets Unlimited Operational Systems and Warehouse</p>

### Project structure

The source code is divided into three packages:

- operations - data generation and exposure (red rectangle)
- warehouse - data ingestion and transformation (blue rectangle)
- model - modules used by both operations and warehouse for the entities and metadata


![overview](./images/project_structure.png)

### Components

### Data Model
The entities are split among the operational systems as follows: 

e-Commerce
- customer
- customer_address
- loyalty_number
- order
- order_line_item

Inventory 
- inventory
- supplier
- product

In-Store
- store
- store_location
- store_sales

Table and Column classes (WidgetsUnlimited.model.metadata.py) describe the layout of these entities 
and hold attributes that are used by the Data Generator and other aspects of the simulation.

### Data Generator

The DataGenerator (WidgetsUnlimited.operations.generator.py) synthesizes sample data for entities modeled by 
Table classes in WidgetsUnlimited.model. When processing a GeneratorRequest, it applies inputs and updates to 
a postgres database (maintaining cumulative state) and returns inputs and updates to the caller, which are then 
routed to the associated operational system.

### Operational Systems
- transformation pipelines in the data warehouse
- orchestration
- containers
- The Simulator -- feeding and running the project


### Phased Rollout
    1. Simulatior infrastructure, Data Generator and Customer Dimension tranformation
    2. Transformations of remaining dimensions and facts
    3. Operational Systems and Ingestions (synchronous one container)
    4. Synchronous separate containers
    5. Cloud Deployment
    6. Asynchronous Orchestration

### Install and run the project

### Prerequisites: 
The simulation will run in any environment with docker, docker-compose and python venv.  
The development environment is:

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
1. pip install -r requirements-operations.txt 
   
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

### demo1 image
### expected result

