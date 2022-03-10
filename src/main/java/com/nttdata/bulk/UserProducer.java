package com.nttdata.bulk;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

@Component
@Slf4j
public class UserProducer {

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("#{'${user.ids}'.split(',')}")
    private List<String> userIds;

    private final ExecutorService service = Executors.newFixedThreadPool(10);

    private final AtomicBoolean runningLoop = new AtomicBoolean(false);

    public void publish(String topic, Integer howMany) {
        if (runningLoop.get()) {
            return;
        }

        if (howMany > 1000) {
            throw new RuntimeException("Cannot create more than 1000 users");
        }

        runningLoop.set(true);
        service.submit(new SingleProducer(bootstrapServers, topic, howMany, userIds));
    }

    public void stopLoop() {
        runningLoop.set(false);
    }

    static class SingleProducer implements Runnable {

        private final KafkaProducer<String, String> jsonProducer;
        private final String topic;
        private final Integer singleBulkSize;

        private final String producerId;

        private final List<String> userIds;

        SingleProducer(String bootstrapServers, String topic,
                       Integer singleBulkSize, List<String> userIds) {
            producerId = "JsonProducer-" + UUID.randomUUID();
            this.singleBulkSize = singleBulkSize;
            this.topic = topic;
            this.userIds = userIds;
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            jsonProducer = new KafkaProducer<>(props);
        }

        @Override
        public void run() {

            try {
                Random r = new Random();
                String message = "{\n" +
                        "  \"cn\": \"%CN%\",\n" +
                        // "  \"timestamp\": \"%TIMESTAMP%\",\n" +
                        "  \"roomNumber\": \"%ROOM%\",\n" +
                        "  \"email\": \"%EMAIL%\"\n" +
                        "}";

                IntStream.range(0, singleBulkSize).forEach(i -> {
                    String m = message.replaceAll("%CN%", "Name" + i + " " + "Lastname" + i)
                            //.replaceAll("%TIMESTAMP%", Instant.now().toString())
                            .replaceAll("%ROOM%", Integer.toString(r.nextInt(10000)))
                            .replaceAll("%EMAIL%", "name" + i + "." + "lastname" + i + "@cmpny.com");

                    String key = userIds.get(i);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, m);
                    jsonProducer.send(record);

                });
            } catch (Exception e) {
                log.error("Failed to publish", e);
            } finally {
                jsonProducer.close();
            }
            log.info("Producer {} is exiting", producerId);
        }

        private String randomFrom(List<String> userIds) {
            Random r = new Random();
            return userIds.get(r.nextInt(userIds.size()));
        }
    }
}
