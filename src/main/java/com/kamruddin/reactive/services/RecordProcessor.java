package com.kamruddin.reactive.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@Profile("!k8s")
public class RecordProcessor implements IRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RecordProcessor.class);

    @Autowired
    private ISseNotificationService sseNotificationService;

    private List<String> users = List.of("kamruddin", "john", "jane", "doe");

    @Scheduled(fixedDelay = 10000)
    public void processRecords() throws UnknownHostException {
        List<String> processedUsers = new ArrayList<>();
        String machineName = Inet4Address.getLocalHost().getHostName();
        for (String user : users) {
            // Simulate processing
            String processedUser = machineName + ", Processed: " + user + ", Timestamp: " + LocalDateTime.now();
            processedUsers.add(processedUser);
            logger.info("Processed user: {}", processedUser);
            sseNotificationService.pushMessageToClient(user, processedUser);
        }

        // Here you can save processedUsers to a database or perform further actions
    }
}
