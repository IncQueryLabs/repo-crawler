package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage
import io.netty.buffer.ByteBufInputStream
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.Json
import java.io.InputStream

/**
 * Vert.x log entry codec base class and utility
 *
 * Instead of manual JSON conversion, this codec can be used to automatically
 *  handle LogMessage types, to ensure both more convenient message passing, and
 *  more performant local-only transfers.
 */
class LogCodec<M : LogMessage> private constructor(
        private val clazz: Class<M>
) : MessageCodec<M, M> {

    companion object {
        private inline fun <reified M : LogMessage> Vertx.registerCodec() {
            val clazz = M::class.java
            eventBus().registerDefaultCodec(clazz, LogCodec(clazz))
        }

        /**
         * Vert.x has trouble with Kotlin sealed class auto-coding,
         *  this helper is used to register codecs for all subtypes of LogMessage.
         *
         * Could be using kotlin-reflections (LogMessage::class.sealedSubclasses),
         *  but instead subtypes are enumerated manually to avoid extra dependencies.
         */
        fun registerForAll(vertx: Vertx) {
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
        return ByteBufInputStream(buffer.byteBuf).let { stream ->
            stream.skip(pos.toLong())
            Json.mapper.readValue(stream as InputStream, clazz)
        }
    }

    override fun transform(log: M): M = log

    override fun name(): String = "LogCodec\$${clazz.simpleName}"

    override fun systemCodecID(): Byte = -1

}