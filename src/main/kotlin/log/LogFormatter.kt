package com.incquerylabs.twc.repo.crawler.log

import com.incquerylabs.twc.repo.crawler.data.LogEntity
import com.incquerylabs.twc.repo.crawler.data.LogMessage

/**
 * Log message formatter service
 */
interface LogFormatter {

    fun format(log: LogMessage): String

}

/**
 * Default log formatter implementation based on previous stdout solution
 */
object DefaultLogFormatter : LogFormatter {

    override fun format(log: LogMessage): String {
        return when (log) {
            is LogMessage.WorkspacesMessage -> formatWorkspaces(log)
            is LogMessage.ResourcesMessage -> formatResources(log)
            is LogMessage.BranchesMessage -> formatBranches(log)
            is LogMessage.RevisionsMessage -> formatRevisions(log)
        }
    }

    private fun LogEntity.format() = "$title (id: $id)"

    private fun formatWorkspaces(message: LogMessage.WorkspacesMessage) = buildString {
        appendln("Received workspaces of repository:")
        message.workspaces.forEach { workspace ->
            append("\t * ")
            appendln(workspace)
        }
    }

    private fun formatResources(message: LogMessage.ResourcesMessage) = buildString {
        with(message) {
            append("Received resources of workspace ")
            append(workspace.format())
            appendln(":")

            resources.forEach { resource ->
                append("\t * ")
                appendln(resource.format())
            }
        }
    }

    private fun formatBranches(message: LogMessage.BranchesMessage) = buildString {
        with(message) {
            append("Received branches of resource ")
            append(resource.format())
            append(" in workspace ")
            append(workspace.format())
            appendln(":")

            branches.forEach { branch ->
                append("\t * ")
                appendln(branch)
            }
        }
    }

    private fun formatRevisions(message: LogMessage.RevisionsMessage) = buildString {
        with(message) {
            append("Received revisions of branch ")
            append(branch.format())
            append(" in resource ")
            append(resource.format())
            append(" in workspace ")
            append(workspace.format())
            appendln(":")

            val idString = revisions.joinToString(prefix = "\t * [", separator = ", ", postfix = "]")
            appendln(idString)
        }
    }

}
