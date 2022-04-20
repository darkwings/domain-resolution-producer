package com.nttdata.bulk;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@Component
@RestController
@Slf4j
public class ProxyLdapProducer {

    String json;
    String jsonSkip;

    Producer<String, String> jsonProducer;

    List<String> uids;

    String bootstrapServers;

    private final ExecutorService service = Executors.newFixedThreadPool(10);
    private final AtomicBoolean runningLoop = new AtomicBoolean(false);

    private ProductionMonitor monitor = new ProductionMonitor("LDAP simulator");

    @SneakyThrows
    public ProxyLdapProducer(@Value("${bootstrap.servers}") String bootstrapServers) {
        URL url = Resources.getResource("proxy-sample.json");
        this.json = Resources.toString(url, Charset.defaultCharset());
        url = Resources.getResource("proxy-sample-skip.json");
        this.jsonSkip = Resources.toString(url, Charset.defaultCharset());
        url = Resources.getResource("users-telecomitalia.txt");
        uids = Resources.readLines(url, Charset.defaultCharset());
        this.bootstrapServers = bootstrapServers;
    }

    @PostMapping("/bulk/proxy/ldap/{topic}/{howMany}/{period}/{n}")
    private String proxies(@PathVariable("topic") String topic,
                           @PathVariable("howMany") int howMany,
                           @PathVariable("period") long period,
                           @PathVariable("n") int n,
                           @RequestParam(value = "factor", defaultValue = "2") int factor) {
        if (runningLoop.get()) {
            return "ALREADY RUNNING\n";
        }

        runningLoop.set(true);
        monitor.start();

        IntStream.range(0, n)
                .forEach(i -> service.submit(new SingleProducer(bootstrapServers,
                        runningLoop, uids, howMany, period, json, jsonSkip, topic, i, factor, monitor)));
        return "OK\n";
    }

    @PostMapping("/bulk/proxy/ldap/_stop")
    public String stopLoop() {
        runningLoop.set(false);
        monitor.stop();
        return "STOPPED\n";
    }

    static class SingleProducer implements Runnable {

        Producer<String, String> jsonProducer;
        AtomicBoolean running;
        List<String> uids;
        Integer singleBulkSize;
        Long delay;
        String json;
        String jsonSkip;
        String topic;
        String producerId;

        int factor;

        ProductionMonitor monitor;

        public SingleProducer(String bootstrapServers, AtomicBoolean running,
                              List<String> uids, Integer singleBulkSize,
                              Long delay, String json, String jsonSkip, String topic, int num, int factor,
                              ProductionMonitor monitor) {
            producerId = "proxy-producer-" + num;
            this.factor = factor;
            this.running = running;
            this.uids = uids;
            this.singleBulkSize = singleBulkSize;
            this.delay = delay;
            this.json = json;
            this.jsonSkip = jsonSkip;
            this.topic = topic;
            this.monitor = monitor;

            val c = EncryptionConfig.createFromSystemProp();

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // props.put(ProducerConfig.CLIENT_ID_CONFIG, "simple-" + UUID.randomUUID().toString());
            // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
            // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

            jsonProducer = new KafkaProducer<>(c.decorateProducer(props));
        }

        @Override
        public void run() {
            AtomicLong counter = new AtomicLong(0);
            log.info("Producer {} is starting - running {}", producerId, running);
            monitor.start();
            try {
                while (running.get()) {
                    val r = new Random();
                    val v = counter.incrementAndGet();
                    IntStream.range(0, singleBulkSize).forEach(i -> {
                        String sJson;
                        String key;
                        if (v % factor == 0) {
                            key = UUID.randomUUID().toString();
                            sJson = jsonSkip
                                    .replaceAll("%CORRELATION_ID%", key)
                                    .replaceAll("%APPLICATION_NAME%", UUID.randomUUID().toString());
                        } else {
                            key = uids.get(r.nextInt(uids.size()));
                            sJson = json.replaceAll("%USER_ID%", key)
                                    .replaceAll("%CORRELATION_ID%", UUID.randomUUID().toString())
                                    .replaceAll("%APPLICATION_NAME%", UUID.randomUUID().toString());

                        }
                        val record = new ProducerRecord<>(topic, key, sJson);
                        monitor.increment();
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
    }
}
