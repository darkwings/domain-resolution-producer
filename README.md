# KAFKA LOAD SIMULATOR

## Start
    
    java -jar target/kafka-producer-0.0.1-SNAPSHOT.jar

or 

    java -Dbootstrap.servers=localhost:9092 -jar target/kafka-producer-0.0.1-SNAPSHOT.jar


On GCP

    java -Dbootstrap.servers=broker-1.europe-west1-b.c.di-devops-lab.internal:9092,broker-2.europe-west1-b.c.di-devops-lab.internal:9092,broker-3.europe-west1-b.c.di-devops-lab.internal:9092 -Dssl.key.password=confluentkeystorestorepass -Dssl.keystore.location=/var/ssl/private/kafka_connect.keystore.jks -Dssl.keystore.password=confluentkeystorestorepass -Dssl.truststore.location=/var/ssl/private/kafka_connect.truststore.jks -Dssl.truststore.password=confluenttruststorepass -Dsecurity.protocol=SSL -jar kafka-producer-0.0.1-SNAPSHOT.jar

To create topics

    ./kafka-topics --bootstrap-server broker-1.europe-west1-b.c.di-devops-lab.internal:9092 --create --topic ldap-enrich-data --partitions 10 --replication-factor 2 --command-config ssl.properties

    ./kafka-console-consumer --bootstrap-server broker-1.europe-west1-b.c.di-devops-lab.internal:9092 --consumer.config ssl.properties --topic ldap-enrich-data --from-beginning

Where `ssl.properties` is

    security.protocol=SSL
    ssl.key.password=confluentkeystorestorepass
    ssl.keystore.location=/var/ssl/private/kafka_broker.keystore.jks
    ssl.keystore.password=confluentkeystorestorepass
    ssl.truststore.location=/var/ssl/private/kafka_broker.truststore.jks
    ssl.truststore.password=confluenttruststorepass
 

## PROXY LOGS Telecomitalia

TIM Users

    curl -X POST http://localhost:8080/proxy/users/ldap-enrich-data

To send data (Selector)
 
    curl -X POST http://localhost:8080/bulk/proxy/selector/logstash-out/1/100/1


To send data (LDAP Enricher)

    curl -X POST http://localhost:8080/bulk/proxy/ldap/ldap-enrich-in/1/100/1

    curl -X POST http://localhost:8080/bulk/proxy/ldap/ldap-enrich-in/15/10/5


## Commands

    curl -X POST http://localhost:8080/bulk/users/cmpny_users/1000

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/_bulk_n_/_pause_

    curl -X POST http://localhost:8080/bulk/_stop

We have 10 producer (fixed number)

To produce 300.000 mess/min (18.000.000 mess/h, 5.000 mess/sec)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/5/10

To produce 840.000 mess/min (50.400.000 mess/h)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/14/10


To produce 900.000 mess/min (54.000.000 mess/h, 15.000 mess/sec)

    curl -X POST http://localhost:8080/bulk/activity/cmpny_activity/15/10

