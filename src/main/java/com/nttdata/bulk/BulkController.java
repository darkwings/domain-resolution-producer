package com.nttdata.bulk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BulkController {

    @Autowired
    private ActivityProducer activityProducer;

    @Autowired
    private UserProducer userProducer;

    @PostMapping("/bulk/users/{topic}/{howMany}")
    private String createUsers(@PathVariable("topic") String topic,
                             @PathVariable("howMany") Integer howMany) {
        userProducer.publish(topic, howMany);
        return "OK";
    }

    @PostMapping("/bulk/activity/{topic}/{bulkSize}/{pause}")
    private String startLoop(@PathVariable("topic") String topic,
                             @PathVariable("bulkSize") Integer bulkSize,
                             @PathVariable("pause") Long pause) {
        activityProducer.loopPublish(topic, bulkSize, pause);
        return "OK";
    }

    @PostMapping("/bulk/_stop")
    private String stopLoop() {
        activityProducer.stopLoop();
        return "OK";
    }
}
