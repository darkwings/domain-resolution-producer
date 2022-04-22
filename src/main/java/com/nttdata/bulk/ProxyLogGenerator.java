package com.nttdata.bulk;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static java.time.format.DateTimeFormatter.ofPattern;

@Component
@RestController
@Slf4j
public class ProxyLogGenerator {

    private static final String PATTERN_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";

    private List<String> uids;

    private List<String> names;

    private ExecutorService service = Executors.newFixedThreadPool(4);

    @SneakyThrows
    public ProxyLogGenerator() {
        URL url = Resources.getResource("users-telecomitalia.txt");
        uids = Resources.readLines(url, Charset.defaultCharset());
        IntStream.range(0, 1000).forEach(i -> {
            uids.add("FAKE_" + i);
        });
        names = List.of("GRIFFONINFRA.GRFDHCP01BA020.STDAPPL_DHCP.L1.WINDOWS.AS.2022-02-25_09-05.1.-4.-4.0.tar.Z#GRIFFONINFRA.STDAPPL.1.1.%HOSTNAME%.2022-02-24_00-01.DHCP.log.zip#24022022.txt",
                "OSCRADIATOR.proxyradius04.STDAPPL_AUTHACCT.L1.LINUX.AS.2022-02-25_00-05.5.-4.-4.0.tar.Z#OSCRADIATOR.STDAPPL.5.5.%HOSTNAME%.2022-02-24_23-59.AUTHACCT.log.gz#OSCRADIATOR.STDAPPL.5.5.proxyradius04.2022-02-24_23-59.AUTHACCT.log",
                "GRIFFON-PE.TELSMTP02RM030.STDAPPL_MSGTRK.L1.WINDOWS.AS.2022-02-24_05-19.24.24.24.0.47604206YW.tar#GRIFFON-PE.STDAPPL.9.24.%HOSTNAME%.2022-02-24_08-00.MSGTRK.log.zip#GRIFFON-PE.STDAPPL.9.24.TELSMTP02RM030.2022-02-24_08-00.MSGTRK.log",
                "Griffon-NavigazioneInternet.TELMCF020RM001.STDAPPL_BROWSING.L1.LINUX.AS.2022-02-25_00-27.11.-4.-4.0.tar.Z#Griffon-NavigazioneInternet.STDAPPL.8.8.%HOSTNAME%.2022-02-24_23-59.BROWSING.log.gz#1...");
    }


    @PostMapping(value = "/bulk/proxy/generate", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.TEXT_PLAIN_VALUE)
    public String generate(@RequestBody ProxyLogGeneratorRequest request) {
        names.forEach(name -> {
            val hName = name.replaceAll("%HOSTNAME%", request.getHostname());
            service.submit(() ->
                    doIt(request.getBasePath() + File.separator + hName,
                            request.getHowMany(), request.getFactor()));
        });
        return "OK\n";
    }


    @SneakyThrows
    void doIt(String path, int howMany, int factor) {
        try {
            log.info("Generating {}", path);
            val writer = new BufferedWriter(
                    new FileWriter(path, true));

            val url = Resources.getResource("proxy-row.txt");
            val row = Resources.toString(url, Charset.defaultCharset());

            val formatter = ofPattern(PATTERN_FORMAT)
                    .withZone(ZoneId.systemDefault());

            val r = new Random();
            int size = uids.size();
            IntStream.range(0, howMany).forEach(i -> {
                val now = Instant.now();
                val uid = i % factor == 0 ? "" : uids.get(r.nextInt(size));
                val rowF = row.replaceAll("%TSTAMP%", formatter.format(now))
                        .replaceAll("%USERID%", uid);
                try {
                    writer.write(rowF + "\n");
                } catch (IOException e) {
                }
            });

            writer.close();
            log.info("Generated {} entries on {}", howMany, path);
        }
        catch (Exception e) {
            log.error("Failed to generate " + path, e);
        }
    }
}
