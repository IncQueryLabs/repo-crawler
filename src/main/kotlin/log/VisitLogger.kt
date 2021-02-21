package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage

interface VisitLogger {

    object NopLogger : VisitLogger {
        override fun logMessage(message: LogMessage) {}
    }

    fun logMessage(message: LogMessage)

    fun onStop() {}

}