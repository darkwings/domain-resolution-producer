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
import org.springframework.web.bind.annotation.RestController;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

@Component
@RestController
@Slf4j
public class ProxySampleProducer {

    String json;

    Producer<String, String> jsonProducer;

    List<String> uids;

    @SneakyThrows
    public ProxySampleProducer(@Value("${bootstrap.servers}") String bootstrapServers) {
        URL url = Resources.getResource("proxy-sample.json");
        this.json = Resources.toString(url, Charset.defaultCharset());
        url = Resources.getResource("users-telecomitalia.txt");
        uids = Resources.readLines(url, Charset.defaultCharset());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "simple-" + UUID.randomUUID().toString());
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        //props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        jsonProducer = new KafkaProducer<>(props);
    }

    @PostMapping("/proxy/{topic}/{userId}")
    private String createUsers(@PathVariable("topic") String topic,
                               @PathVariable("userId") String userId) {
        val j = json.replaceAll("%USER_ID%", userId)
                .replaceAll("%CORRELATION_ID%", UUID.randomUUID().toString());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, userId, j);
        jsonProducer.send(record);
        return "OK";
    }

    @PostMapping("/proxy/{topic}/bulk/{bulk}")
    private String createUsers(@PathVariable("topic") String topic,
                               @PathVariable("bulk") int bulk) {
        val r = new Random();
        log.info("Sending {} messages", bulk);
        IntStream.range(0, bulk).forEach(i -> {

            val userId = uids.get(r.nextInt(uids.size()));
            log.info("Sending data about {}", userId);
            val j = json.replaceAll("%USER_ID%", userId)
                    .replaceAll("%CORRELATION_ID%", UUID.randomUUID().toString());
            val record = new ProducerRecord<>(topic, userId, j);
            jsonProducer.send(record);
        });
        return "OK";
    }

}
