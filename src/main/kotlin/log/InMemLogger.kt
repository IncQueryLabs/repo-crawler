package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.*
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.nio.file.Path
import java.util.*

class InMemLogger(
        storage: () -> MutableList<LogMessage> = ::LinkedList
) : VisitLogger {

    private val store = storage()
    val storedLogs: List<LogMessage>
        get() = store

    override fun logMessage(message: LogMessage) {
        store += message
    }

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

    @Throws(IOException::class)
    fun writeOut(writer: PrintWriter, formatter: LogFormatter = DefaultLogFormatter) {
        store.forEach { message ->
            writer.print(formatter.format(message))
        }
        writer.flush()
    }

}