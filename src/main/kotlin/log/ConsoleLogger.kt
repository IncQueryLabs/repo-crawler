package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage

/**
 * stdout logger
 *
 * Writes the received log entries to the standard output as they are received.
 *
 * @param formatter the log formatter used
 */
class ConsoleLogger(
        private val formatter: LogFormatter = DefaultLogFormatter
) : VisitLogger {

    override fun logMessage(message: LogMessage) {
        print(formatter.format(message))
    }

    override fun onStop() {
        System.out.flush()
    }

}