package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage

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