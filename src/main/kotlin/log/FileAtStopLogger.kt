package com.incquerylabs.twc.repo.crawler.log

import java.nio.file.Path

/**
 * Post-close file log wrapper using in-memory store
 *
 * Stores the received log messages in an in-mem log store.
 * After the run is over, writes the log entries to the given file.
 *
 * @param filePath the file to write into
 * @param formatter the log formatter to use
 * @param createIfNeeded whether to create a new file if needed
 * @param appendIfExists whether to append the file if already present
 * @param backingLogger the in-mem backer used
 */
class FileAtStopLogger(
        private val filePath: Path,
        private val createIfNeeded: Boolean = true,
        private val appendIfExists: Boolean = true,
        private val formatter: LogFormatter = DefaultLogFormatter,
        private val backingLogger: InMemLogger = InMemLogger()
) : VisitLogger by backingLogger {

    /**
     * Actually write contents to file
     */
    override fun onStop() {
        backingLogger.writeToFile(filePath, formatter, createIfNeeded, appendIfExists)
    }

}