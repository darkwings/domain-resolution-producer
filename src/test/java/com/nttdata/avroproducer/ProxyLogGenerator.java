package com.nttdata.avroproducer;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static java.time.format.DateTimeFormatter.ofPattern;

public class ProxyLogGenerator {

    private static final String PATH = "/Users/ETORRIFUT/work/proxy3.log";
    private static final Integer HOW_MANY = 4;
    private static final Integer FACTOR = 2;

    private static final String PATTERN_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";

    private List<String> uids;

    @BeforeEach
    void beforeEach() throws IOException {
        val url = Resources.getResource("users-telecomitalia.txt");
        uids = Resources.readLines(url, Charset.defaultCharset());
        IntStream.range(0, 1000).forEach(i -> {
            uids.add("FAKE_"+ i);
        });
    }

    @SneakyThrows
    @Test
    //@Disabled
    void doIt() {

        val writer = new BufferedWriter(
                new FileWriter(PATH, true));

        val url = Resources.getResource("proxy-row.txt");
        val row = Resources.toString(url, Charset.defaultCharset());

        val formatter = ofPattern(PATTERN_FORMAT)
                .withZone(ZoneId.systemDefault());

        val r = new Random();
        int size = uids.size();
        IntStream.range(0, HOW_MANY).forEach(i -> {
            val now = Instant.now();
            val uid = i % FACTOR == 0 ? "": uids.get(r.nextInt(size));
            val rowF = row.replaceAll("%TSTAMP%", formatter.format(now))
                    .replaceAll("%USERID%", uid);
            try {
                writer.write(rowF + "\n");
            } catch (IOException e) {
            }
        });

        writer.close();
    }
}
