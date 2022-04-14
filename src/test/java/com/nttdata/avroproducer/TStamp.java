package com.nttdata.avroproducer;

import lombok.val;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

public class TStamp {

    private static final String PATTERN_FORMAT = "dd/MMM/yyyy:hh:mm:ss Z";

    @Test
    @Disabled
    void test() {

        val i = Instant.now();
        System.out.println(i);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(PATTERN_FORMAT)
                //.withLocale(Locale.ITALIAN)
                .withZone(ZoneId.systemDefault())
        ;

        System.out.println(formatter.format(i));
        // 18/Feb/2022:23:59:01 +0100
    }

    @Test
    @Disabled
    public void givenInstant_whenUsingDateTimeFormatter_thenFormat() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(PATTERN_FORMAT)
                .withZone(ZoneId.systemDefault());

        Instant instant = Instant.parse("2022-01-15T18:35:24.00Z");
        String formattedInstant = formatter.format(instant);

        assertThat(formattedInstant).isEqualTo("15/02/2022");
    }
}
