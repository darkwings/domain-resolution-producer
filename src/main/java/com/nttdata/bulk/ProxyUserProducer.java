package com.nttdata.bulk;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

@RestController
@Slf4j
public class ProxyUserProducer {

    static final ObjectMapper MAPPER = new ObjectMapper();

    String bootstrapServers;

    List<String> uids;

    Producer<String, String> producer;


    @Autowired
    public ProxyUserProducer(@Value("${bootstrap.servers}") String bootstrapServers) throws IOException {
        this.bootstrapServers = bootstrapServers;
        val c = EncryptionConfig.createFromSystemProp();

        URL url = Resources.getResource("users-telecomitalia.txt");
        uids = Resources.readLines(url, Charset.defaultCharset());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(c.decorateProducer(props));
    }


    @PostMapping("/proxy/users/{topic}")
    public String storeUser(@PathVariable("topic") String topic) {

        uids.forEach(uid -> {
            User u = User.builder()
                    .cn("User " + uid)
                    .uid(uid)
                    .department("Department of " + uid)
                    .displayName("Display of " + uid)
                    .mail(uid + "@telecomitalia.fake.it")
                    .manager("Manager of " + uid)
                    .build();
            try {
                producer.send(new ProducerRecord<>(topic, uid, MAPPER.writeValueAsString(u)));
            } catch (JsonProcessingException e) {
            }
        });
        return "DONE\n";
    }
}
