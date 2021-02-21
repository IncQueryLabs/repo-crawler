package com.incquerylabs.twc.repo.crawler.log

import java.nio.file.Path

class FileAtStopLogger(
        private val filePath: Path,
        private val createIfNeeded: Boolean = true,
        private val appendIfExists: Boolean = true,
        private val formatter: LogFormatter = DefaultLogFormatter,
        private val backingLogger: InMemLogger = InMemLogger()
) : VisitLogger by backingLogger {

    override fun onStop() {
        backingLogger.writeToFile(filePath, formatter, createIfNeeded, appendIfExists)
    }

}