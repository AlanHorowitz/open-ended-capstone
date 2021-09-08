##  Widgets Unlimited - A data warehousing simulation

### Overview
    - what is it
    - what is the motivation
    - Practice data transforming pipelines for different use cases
    - A convenient testing infratructure where you could plug in different data siyrces and ingestions
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
    4. Cloud Deployment
    5. Asynchronous Orchestration

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

