# KAFKA LOAD SIMULATOR

## Start
    
    java -jar target/kafka-producer-0.0.1-SNAPSHOT.jar

## Commands

    curl -X POST http://localhost:8080/bulk/users/cmpny_users/1000

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/_bulk_n_/_pause_

    curl -X POST http://localhost:8080/bulk/_stop

We have 10 producer (fixed number)

To produce 300.000 mess/min (18.000.000 mess/h, 5000 mess/sec)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/5/10

To produce 840.000 mess/min (50.400.000 mess/h)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/14/10


To produce 900.000 mess/min (54.000.000 mess/h)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/15/10
