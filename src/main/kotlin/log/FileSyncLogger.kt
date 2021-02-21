package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage
import java.io.FileWriter
import java.io.PrintWriter
import java.nio.file.Path

class FileSyncLogger(
        filePath: Path,
        private val formatter: LogFormatter = DefaultLogFormatter,
        createIfNeeded: Boolean = true,
        appendIfExists: Boolean = true
) : VisitLogger, AutoCloseable {

    private val fileWriter: FileWriter
    private val writer: PrintWriter

    init {
        with(filePath.toFile()) {
            if (!exists()) {
                if (!createIfNeeded) {
                    error("Log '${absolutePath}' file does not exists and create-if-needed not set!")
                }
            }

            fileWriter = FileWriter(this, appendIfExists)
            writer = PrintWriter(fileWriter)
        }
    }

    override fun logMessage(message: LogMessage) {
        writer.print(formatter.format(message))
    }

    fun flush() {
        writer.flush()
    }

    override fun onStop() {
        close()
    }

    override fun close() {
        fileWriter.use {
            writer.use {
                it.flush()
            }
        }
    }

}