package com.kamruddin.reactive.services;

import java.net.UnknownHostException;

/**
 * Interface for record processing services.
 * Implementations can be single-instance or distributed across multiple pods.
 */
public interface IRecordProcessor {

    /**
     * Process records according to the implementation strategy.
     * This method is typically called by a scheduler.
     *
     * @throws UnknownHostException if hostname cannot be determined
     */
    void processRecords() throws UnknownHostException;
}
