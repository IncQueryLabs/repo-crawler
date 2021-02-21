package com.incquerylabs.twc.repo.crawler.data

data class LogEntity(
        val id: String,
        val title: String
)

/**
 * Structure log messages for crawling
 */
sealed class LogMessage {

    data class WorkspacesMessage(
            val workspaces: Collection<String>
    ) : LogMessage()

    data class ResourcesMessage(
            val workspace: LogEntity,
            val resources: Collection<LogEntity>
    ) : LogMessage()

    data class BranchesMessage(
            val workspace: LogEntity,
            val resource: LogEntity,
            val branches: Collection<String>
    ) : LogMessage()

    data class RevisionsMessage(
            val workspace: LogEntity,
            val resource: LogEntity,
            val branch: LogEntity,
            val revisions: Collection<String>
    ) : LogMessage()

}