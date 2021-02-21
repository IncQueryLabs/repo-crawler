package com.incquerylabs.twc.repo.crawler.verticles

import com.incquerylabs.twc.repo.crawler.data.*
import com.incquerylabs.twc.repo.crawler.data.LogMessage.RevisionsMessage
import com.incquerylabs.twc.repo.crawler.log.ConsoleLogger
import com.incquerylabs.twc.repo.crawler.log.InMemLogger
import com.incquerylabs.twc.repo.crawler.log.VisitLogger
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject

class LogVerticle(
        private val backingLoggers: Collection<VisitLogger> = listOf(ConsoleLogger())
) : AbstractVerticle() {

    override fun start() {
        vertx.eventBus().consumer<LogMessage>(TWCLOG_ADDRESS) { message ->
            backingLoggers.forEach { logger ->
                logger.logMessage(message.body() ?: return@consumer)
            }
        }
    }

    override fun stop() {
        backingLoggers.forEach { logger ->
            logger.onStop()
        }
    }

}