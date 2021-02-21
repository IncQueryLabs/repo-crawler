package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.*
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.nio.file.Path
import java.util.*

/**
 * In-memory log store
 *
 * Stores the received log messages in a backing list.
 * @param storage can be used to alter the internal storage used.
 *  For concurrent logging, use a thread-safe backing list implementation
 *  (e.g. with java.util.Collections.synchronizedList)
 *
 * The accumulated log entries can be viewed or written directly to a stream or file.
 */
class InMemLogger(
        storage: () -> MutableList<LogMessage> = ::LinkedList
) : VisitLogger {

    private val store = storage()

    /** A read-only view of the accumulated logs */
    val storedLogs: List<LogMessage>
        get() = store

    override fun logMessage(message: LogMessage) {
        store += message
    }

    /**
     * Writes the log entries to a given file using the provided formatter
     *
     * @param path the file to write into
     * @param formatter the log formatter to use
     * @param createIfNeeded whether to create a new file if needed
     * @param appendIfExists whether to append the file if already present
     */
    @Throws(IOException::class)
    fun writeToFile(
            path: Path,
            formatter: LogFormatter = DefaultLogFormatter,
            createIfNeeded: Boolean = true,
            appendIfExists: Boolean = true
    ): Boolean {
        return with(path.toFile()) {
            if (!exists()) {
                if (!createIfNeeded) {
                    return false
                }
            }

            FileWriter(this, appendIfExists).use { fileWriter ->
                PrintWriter(fileWriter).use { writer ->
                    writeOut(writer, formatter)
                    true
                }
            }
        }
    }

    /**
     * Writes the log entries to a given stream (does no close it afterward)
     *
     * @param writer the writer to use
     * @param formatter the log formatter to use
     */
    @Throws(IOException::class)
    fun writeOut(writer: PrintWriter, formatter: LogFormatter = DefaultLogFormatter) {
        store.forEach { message ->
            writer.print(formatter.format(message))
        }
        writer.flush()
    }

}