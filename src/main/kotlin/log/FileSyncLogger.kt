package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogMessage
import java.io.FileWriter
import java.io.PrintWriter
import java.nio.file.Path

/**
 * Synchronous file logger
 *
 * Writes the log entries into a file as they are received.
 *
 * @param filePath the file to write into
 * @param formatter the log formatter to use
 * @param createIfNeeded whether to create a new file if needed
 * @param appendIfExists whether to append the file if already present
 */
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
        flush()
    }

    fun flush() {
        writer.flush()
    }

    /**
     * Force flush and close writers
     */
    override fun onStop() {
        close()
    }

    /**
     * Close open writers
     */
    override fun close() {
        fileWriter.use {
            writer.use {
                it.flush()
            }
        }
    }

}