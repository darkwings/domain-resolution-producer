package com.nttdata.bulk;

import lombok.extern.slf4j.Slf4j;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class ProductionMonitor {

    AtomicInteger sent = new AtomicInteger(0);
    Timer timer = new Timer();
    String name;

    public ProductionMonitor(String name) {
        this.name = name;
    }

    void increment() {
        sent.incrementAndGet();
    }

    public void stop() {
        if (timer != null) {
            timer.cancel();
        }
    }

    public void start() {
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                log.info("{} sent {} message per sec", name, sent.get());
                sent.set(0);
            }
        }, 1000L, 1000L);
    }
}
