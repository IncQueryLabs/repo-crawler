package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage

/**
 * Common log processor interface
 */
interface VisitLogger {

    /**
     * No-operation logger (mainly for testing)
     * Discards the received log entries.
     */
    object NopLogger : VisitLogger {
        override fun logMessage(message: LogMessage) {}
    }

    /**
     * Handles individual log entries
     */
    fun logMessage(message: LogMessage)

    /**
     * Signals the end of log accumulation (e.g. for flushing)
     */
    fun onStop() {}

}