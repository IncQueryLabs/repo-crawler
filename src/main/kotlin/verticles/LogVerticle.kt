package com.incquerylabs.twc.repo.crawler.verticles

import com.incquerylabs.twc.repo.crawler.data.*
import com.incquerylabs.twc.repo.crawler.log.ConsoleLogger
import com.incquerylabs.twc.repo.crawler.log.VisitLogger
import io.vertx.core.AbstractVerticle

/**
 * Vert.x log handling verticle
 *
 * Listens to log messages asynchronously on the address defined by TWCLOG_ADDRESS,
 *  handles incoming messages using the give loggers.
 *
 * @param backingLoggers the loggers to use for processing, all receive every message
 */
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