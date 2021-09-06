##  Widgets Unlimited - A data warehousing simulation

###  A simulated data warehouse in the retail domain.  Add records to a product table on a postgres source system and extract them to a MySQL target system.

### Prerequisites: The simulation will run in any environment with docker, docker-compose and python venv.  The development 
### environment was:

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

###  Issue the following commands from the open-ended-capstone directory to start the databases, build and run:

```
docker-compose up -d
docker build -t retail_dw .
docker run -t retail_dw [demo1/demo2/demo3]
```

### Demo 1 
Initial load of 5000 records followed by five incremental loads, each with 200 inserts and 50 updates.  The demo may be run repeatedly.

###   Expected output on clean system :

```
6000 inserts and 250 updates processed.
```

### Demo 2 
Initial load of 5000 records followed by five incremental loads, each with 200 inserts and 50 updates.  Following each of the incremental loads, extract data from the source system and apply it to the target system. The demo may be run repeatedly.

###   Expected output on clean system :

```
5000 inserts and 0 updates processed at source: 2021-03-31 01:44:15.664063.
200 inserts and 50 updates processed at source: 2021-03-31 01:44:19.262280.
5200 inserts and 0 updates processed at target: From: 1980-01-01 00:00:00 To: 2021-03-31 01:44:19.262280 
200 inserts and 50 updates processed at source: 2021-03-31 01:44:24.846703.
200 inserts and 50 updates processed at target: From: 2021-03-31 01:44:19.262280 To: 2021-03-31 01:44:24.846703 
200 inserts and 50 updates processed at source: 2021-03-31 01:44:28.009200.
200 inserts and 50 updates processed at target: From: 2021-03-31 01:44:24.846703 To: 2021-03-31 01:44:28.009200 
200 inserts and 50 updates processed at source: 2021-03-31 01:44:31.171240.
200 inserts and 50 updates processed at target: From: 2021-03-31 01:44:28.009200 To: 2021-03-31 01:44:31.171240 
200 inserts and 50 updates processed at source: 2021-03-31 01:44:34.388203.
200 inserts and 50 updates processed at target: From: 2021-03-31 01:44:31.171240 To: 2021-03-31 01:44:34.388203 
6000 inserts and 250 updates processed at source.
6000 inserts and 200 updates processed at target.
```

### Demo 3 
Demonstrate three days operation of ETL system using product, store and store_sales tables.  

###   Expected output on clean system :

```
-------------------
Day 1 of operations
-------------------
5000 inserts and 0 updates for table product processed at source: 2021-04-14 22:47:19.886354
40 inserts and 0 updates for table store processed at source: 2021-04-14 22:47:20.426648
5000 inserts and 0 updates for table product processed at target: From: 2021-04-14 22:43:43.924259 To: 2021-04-14 22:47:19.886354 
40 inserts and 0 updates for table store processed at target: From: 2021-04-14 22:43:43.947473 To: 2021-04-14 22:47:20.426648 
-------------------
Day 2 of operations
-------------------
5 inserts and 50 updates for table product processed at source: 2021-04-14 22:47:22.115460
0 inserts and 2 updates for table store processed at source: 2021-04-14 22:47:22.145321
50000 inserts and 0 updates for table store_sales processed at source: 2021-04-14 22:47:22.152464
5 inserts and 50 updates for table product processed at target: From: 2021-04-14 22:47:19.886354 To: 2021-04-14 22:47:22.115460 
0 inserts and 2 updates for table store processed at target: From: 2021-04-14 22:47:20.426648 To: 2021-04-14 22:47:22.145321 
50000 inserts and 0 updates for table store_sales processed at target: From: 2021-04-14 22:43:43.949354 To: 2021-04-14 22:47:22.152464 
-------------------
Day 3 of operations
-------------------
10 inserts and 30 updates for table product processed at source: 2021-04-14 22:47:43.668894
1 inserts and 0 updates for table store processed at source: 2021-04-14 22:47:43.687859
50000 inserts and 0 updates for table store_sales processed at source: 2021-04-14 22:47:43.689269
10 inserts and 30 updates for table product processed at target: From: 2021-04-14 22:47:22.115460 To: 2021-04-14 22:47:43.668894 
1 inserts and 0 updates for table store processed at target: From: 2021-04-14 22:47:22.145321 To: 2021-04-14 22:47:43.687859 
50000 inserts and 0 updates for table store_sales processed at target: From: 2021-04-14 22:47:22.152464 To: 2021-04-14 22:47:43.689269 
```

```
### To inspect the product table from the Postgres CLI:
```
docker run --rm -it postgres psql -h 172.17.0.1 -d retaildw -U user1

Enter user1 as password
``` 

### To inspect the product table from the MySQL CLI:
```
docker run -it mysql:8.0.23 mysql -h 172.17.0.1 -D retaildw -u user1 -p

Enter user1 as password
```

## Note: This program depends on the docker host being at address 172.17.0.1

