package com.nttdata.bulk;


import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
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
public class ActivityProducer {

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("#{'${user.ids}'.split(',')}")
    private List<String> userIds;

    @Value("#{'${test.ip}'.split(',')}")
    private List<String> ips;

    // @Value("#{'${test.domains}'.split(',')}")
    private List<String> domains;

    private final ExecutorService service = Executors.newFixedThreadPool(10);

    private final AtomicBoolean runningLoop = new AtomicBoolean(false);

    public ActivityProducer() throws IOException {
        URL url = Resources.getResource("opendns-top-domains.txt");
        String s = Resources.toString(url, Charset.defaultCharset());
        domains = new ArrayList<>();
        try(BufferedReader r = new BufferedReader(new StringReader(s))) {
            String line;
            while ((line = r.readLine()) != null) {
                domains.add(line);
            }
        }
    }

    public void loopPublish(String topic, Integer singleBulkSize, Long delay) {
        if (runningLoop.get()) {
            return;
        }

        runningLoop.set(true);

        long waitToStart = 1000L;
        IntStream.range(0, 10)
                .forEach(i -> service.submit(new SingleProducer(bootstrapServers,
                        topic, singleBulkSize, delay,
                        waitToStart * i, runningLoop, userIds, ips, domains)));
    }

    public void stopLoop() {
        runningLoop.set(false);
    }

    static class SingleProducer implements Runnable {

        private final KafkaProducer<String, String> jsonProducer;
        private final String topic;
        private final Integer singleBulkSize;
        private final Long delay;
        private final Long waitToStart;
        private final AtomicBoolean running;
        private final String producerId;
        private final List<String> userIds;
        private final List<String> ips;
        private final List<String> domains;

        SingleProducer(String bootstrapServers, String topic,
                       Integer singleBulkSize,
                       Long delay, Long waitToStart,
                       AtomicBoolean running,
                       List<String> userIds,
                       List<String> ips,
                       List<String> domains) {
            producerId = "JsonProducer-" + UUID.randomUUID();
            this.singleBulkSize = singleBulkSize;
            this.topic = topic;
            this.delay = delay;
            this.waitToStart = waitToStart;
            this.running = running;
            this.userIds = userIds;
            this.ips = ips;
            this.domains = domains;

            val c = EncryptionConfig.createFromSystemProp();
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
            //props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            jsonProducer = new KafkaProducer<>(c.decorateProducer(props));
        }

        @Override
        public void run() {
            try {
                Thread.sleep(waitToStart);
                String message = "{\n" +
                        "  \"userid\": \"%USER_ID%\",\n" +
                        "  \"message\": \"%MESSAGE%\",\n" +
                        "  \"date\": \"%TIMESTAMP%\",\n" +
                        "  \"action\": \"This is the action\",\n" +
                        "  \"ip\": \"%IP%\"\n," +
                        "  \"domain\": \"%DOMAIN%\"\n" +
                        "}";

                while (running.get()) {
                    String json = message.replaceAll("%USER_ID%", randomFrom(userIds))
                            .replaceAll("%MESSAGE%", "This is the message " + UUID.randomUUID())
                            .replaceAll("%TIMESTAMP%", Instant.now().toString())
                            .replaceAll("%IP%",randomFrom(ips))
                            .replaceAll("%DOMAIN%",randomFrom(domains));
                    IntStream.range(0, singleBulkSize).forEach(i -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                                UUID.randomUUID().toString(), json);
                        jsonProducer.send(record);
                    });
                    Thread.sleep(delay);
                }
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
