//package com.kamruddin.reactive.config;
//
//import com.kamruddin.reactive.services.MongoStreamEvents;
//import com.kamruddin.reactive.services.UserNotificationConsumer;
//import jakarta.annotation.PostConstruct;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//@Service
//public class NotificationConfig {
//
//    private static final Logger logger = LoggerFactory.getLogger(NotificationConfig.class);
//
//    @Autowired
//    private UserNotificationConsumer userNotificationConsumer;
//
//    @Autowired
//    private MongoStreamEvents mongoStreamEvents;
//
//    public NotificationConfig() {
//
//    }
//
//    @PostConstruct
//    public void init() {
//        if(mongoStreamEvents != null) {
//            // Start distributed monitoring with the reactive UserNotificationConsumer
//            mongoStreamEvents.startDistributedMonitoring("messages", userNotificationConsumer);
////            mongoStreamEvents.startDistributedMonitoringWithStreams("messages", streamNotificationConsumer);
//            logger.info("Started distributed monitoring for messages collection with reactive notifications");
//        } else {
//            logger.error("MongoStreamEvents is null, cannot start distributed monitoring.");
//        }
//    }
//}
