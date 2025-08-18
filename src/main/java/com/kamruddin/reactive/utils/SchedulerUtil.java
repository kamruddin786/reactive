package com.kamruddin.reactive.utils;

public class SchedulerUtil {
    public static boolean enableScheduler = false;
    private SchedulerUtil(){}

    public static void enableScheduler() {
        enableScheduler = true;
    }
    public static void disableScheduler() {
        enableScheduler = false;
    }

}
