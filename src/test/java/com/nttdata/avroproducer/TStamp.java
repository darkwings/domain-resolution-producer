package com.nttdata.avroproducer;

import lombok.val;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class TStamp {

    @Test
    void test() {

        val i = Instant.now();
        System.out.println(i);
    }
}
