package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.Json

class LogCodec<M : LogMessage> private constructor(
        private val clazz: Class<M>
) : MessageCodec<M, M> {

    companion object {
        private inline fun < reified M : LogMessage> Vertx.registerCodec() {
            val clazz = M::class.java
            val codec = LogCodec(clazz)
            eventBus().registerDefaultCodec(clazz, codec)
        }

        fun registerForAll(vertx: Vertx) {
            // Could use kotlin-reflections (LogMessage::class.sealedSubclasses)
            vertx.registerCodec<LogMessage.WorkspacesMessage>()
            vertx.registerCodec<LogMessage.ResourcesMessage>()
            vertx.registerCodec<LogMessage.BranchesMessage>()
            vertx.registerCodec<LogMessage.RevisionsMessage>()
        }
    }

    override fun encodeToWire(buffer: Buffer, log: M) {
        buffer.appendBuffer(Json.encodeToBuffer(log))
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): M {
        return Json.decodeValue(buffer, clazz)
    }

    override fun transform(log: M): M = log

    override fun name(): String = "LogCodec\$${clazz.simpleName}"

    override fun systemCodecID(): Byte = -1

}