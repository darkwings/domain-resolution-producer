package com.nttdata.avroproducer;

import com.nttdata.bulk.EncryptionConfig;
import com.nttdata.messages.User;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

@RestController
public class DemoController {

    private KafkaProducer<String, User> avroProducer;

    private KafkaProducer<String, String> jsonProducer;

    @Value("${avro.topic}")
    private String avroTopic;

    @Value("${json.topic}")
    private String jsonTopic;

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${gcp.project}")
    private String gcpProjectId;

    @Value("${gcp.topic}")
    private String pubsubTopic;

    private static final String TEMPLATE = "{\n" +
            "  \"orderid\": \"%ID%\",\n" +
            "  \"itemid\": \"%ITEMID%\",\n" +
            "  \"address\": {\n" +
            "    \"city\": \"Mountain View\",\n" +
            "    \"state\": \"CA\",\n" +
            "    \"zipcode\": 94041\n" +
            "  }\n" +
            "}";

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "JsonProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        val c = EncryptionConfig.createFromSystemProp();
        jsonProducer = new KafkaProducer<>(c.decorateProducer(props));

        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        avroProducer = new KafkaProducer<>(c.decorateProducer(props));
    }

    @PostMapping("/publish/avro/{n}")
    public String avro(@PathVariable("n") Integer n) {
        IntStream.range(0, n).forEach(i -> {
            ProducerRecord<String, User> record = new ProducerRecord<>(avroTopic, UUID.randomUUID().toString(),
                    User.newBuilder()
                            .setName(UUID.randomUUID().toString())
                            .setFavoriteNumber(i)
                            .setFavoriteColor("red")
                            .build());
            avroProducer.send(record);
        });
        return "OK";
    }

    @PostMapping("/publish/json/{n}")
    public String json(@PathVariable("n") Integer n) {
        IntStream.range(0, n).forEach(i -> {
            String s = TEMPLATE.replaceAll("%ID%", UUID.randomUUID().toString())
                    .replaceAll("%ITEMID%", UUID.randomUUID().toString());
            ProducerRecord<String, String> record = new ProducerRecord<>(jsonTopic, UUID.randomUUID().toString(), s);
            jsonProducer.send(record);
        });
        return "OK";
    }

    @PostMapping("/publish/jsoncrack/{n}/{text}")
    public String jsonCrack(@PathVariable("text") String text, @PathVariable("n") Integer n) {

        // 1 messaggio OK
        String key = UUID.randomUUID().toString();
        IntStream.range(0, n).forEach(i -> {
            String s = TEMPLATE.replaceAll("%ID%", UUID.randomUUID().toString())
                    .replaceAll("%ITEMID%", text + "-" + i);
            ProducerRecord<String, String> record = new ProducerRecord<>(jsonTopic, key, s);
            jsonProducer.send(record);
            record = new ProducerRecord<>(jsonTopic, key, text + "-" + i);
            jsonProducer.send(record);
        });

        return "OK";
    }

    @PostMapping("/publish/jsoncrack/all/{n}/{text}")
    public String jsonCrackAll(@PathVariable("text") String text, @PathVariable("n") Integer n) {

        // 1 messaggio OK
        IntStream.range(0, n).forEach(i -> {
            String key = UUID.randomUUID().toString();
            ProducerRecord<String, String> record = new ProducerRecord<>(jsonTopic, key, text + "-" + i);
            jsonProducer.send(record);
        });

        return "OK";
    }

    @PostMapping("/publish")
    public String doPub() throws IOException, ExecutionException, InterruptedException {
        publishAvroRecordsExample(gcpProjectId, pubsubTopic);
        return "OK";
    }

    public static void publishAvroRecordsExample(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException {

        Encoding encoding;

        TopicName topicName = TopicName.of(projectId, topicId);

        // Get the topic encoding type.
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            encoding = topicAdminClient.getTopic(topicName).getSchemaSettings().getEncoding();
        }

        // Instantiate an avro-tools-generated class
        User state = User.newBuilder().setName("Alaska").setFavoriteNumber(666).setFavoriteColor("red").build();

        Publisher publisher = null;

        block:
        try {
            publisher = Publisher.newBuilder(topicName).build();

            // Prepare to serialize the object to the output stream.
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

            Encoder encoder;

            // Prepare an appropriate encoder for publishing to the topic.
            switch (encoding) {
                case BINARY:
                    System.out.println("Preparing a BINARY encoder...");
                    encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /*reuse=*/ null);
                    break;

                case JSON:
                    System.out.println("Preparing a JSON encoder...");
                    encoder = EncoderFactory.get().jsonEncoder(User.getClassSchema(), byteStream);
                    break;

                default:
                    break block;
            }

            // Encode the object and write it to the output stream.
            state.customEncode(encoder);
            encoder.flush();

            // Publish the encoded object as a Pub/Sub message.
            ByteString data = ByteString.copyFrom(byteStream.toByteArray());
            PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
            System.out.println("Publishing message: " + message);

            ApiFuture<String> future = publisher.publish(message);
            System.out.println("Published message ID: " + future.get());

        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}
