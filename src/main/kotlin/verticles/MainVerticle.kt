package com.incquerylabs.twc.repo.crawler.verticles

import com.incquerylabs.twc.repo.crawler.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject

class MainVerticle(
    val configuration: MainConfiguration
) : AbstractVerticle() {
    var sum = 0
    var number = 0
    var s = 0
    var queries = 0

    companion object {
        @Suppress("UNCHECKED_CAST")
        private fun asJson(content: Any?): JsonObject {
            return JsonObject(content as Map<String, Any>)
        }
    }

    override fun start() {
        val twcMap = vertx.sharedData().getLocalMap<Any, Any>(TWCMAP)
        val requestSingleElements = configuration.requestSingleElement

        val eb = vertx.eventBus()

        fun log(logMessage: LogMessage) {
            eb.send(TWCLOG_ADDRESS, logMessage)
        }

        vertx.setPeriodic(1000) {
            vertx.sharedData().getCounter(QUERIES) { queriesR ->
                if (queriesR.succeeded()) {
                    val queriesC = queriesR.result()
                    queriesC.get { queriesV ->
                        this.queries = queriesV.result().toInt()

                        vertx.sharedData().getCounter("sum") { sumR ->
                            if (sumR.succeeded()) {
                                val sumC = sumR.result()
                                sumC.get { sumV ->
                                    if (sumV.succeeded()) {
                                        this.sum = sumV.result().toInt()

                                        vertx.sharedData().getCounter("number") { numR ->
                                            if (numR.succeeded()) {
                                                val numC = numR.result()
                                                numC.get { numV ->
                                                    if (numV.succeeded()) {
                                                        number = numV.result().toInt()
                                                        println("Total: ${this.queries}/${this.sum}/${++s} query/response/sec | Now: $number elem | AvgSpeed: ${this.sum / (s)} elem/sec")
                                                        numC.compareAndSet(number.toLong(), 0, {})

                                                        if (this.queries != 0 && this.sum != 0 && this.queries == this.sum) {
                                                            eb.send(
                                                                TWCVERT_ADDRESS,
                                                                JsonObject.mapFrom(
                                                                    Message(
                                                                        LOGOUT,
                                                                        JsonObject()
                                                                    )
                                                                )
                                                            )
                                                        }
                                                    }
                                                }
                                            } else {
                                                error("Counter: queriesR not available.")
                                            }
                                        }
                                    }
                                }
                            } else {
                                error("Counter: queriesR not available.")
                            }
                        }


                    }
                } else {
                    // Something went wrong!
                }
            }

        }
        eb.send(
            TWCVERT_ADDRESS, JsonObject.mapFrom(
                Message(
                    LOGIN,
                    configuration.user
                )
            ))
        eb.consumer<Any>(TWCMAIN_ADDRESS) { message ->

            val messageData = (message.body() as JsonObject).mapTo(Message::class.java)

            when (messageData.event) {
                LOGGED_IN -> {
                    println("Login complete (user: ${twcMap[USER]}, session:  ${twcMap[SESSION]})")

                    val workspaceId = twcMap[WORKSPACE_ID]
                    val resourceId = twcMap[RESOURCE_ID]
                    val branchId = twcMap[BRANCH_ID]
                    val revision = twcMap[REVISION]
                    when {
                        workspaceId == null -> eb.send(
                            TWCVERT_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    GET_WORKSPACES,
                                    JsonObject()
                                )
                            )
                        )
                        resourceId == null -> eb.send(
                            TWCVERT_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    GET_RESOURCES,
                                    JsonObject().put(
                                        WORKSPACE_ID,
                                        workspaceId
                                    )
                                )
                            )
                        )
                        branchId == null -> eb.send(
                            TWCVERT_ADDRESS, JsonObject.mapFrom(
                                Message(
                                    GET_BRANCHES,
                                    JsonObject()
                                        .put(
                                            WORKSPACE_ID,
                                            workspaceId
                                        )
                                        .put(
                                            RESOURCE_ID,
                                            resourceId
                                        )
                                )
                            )
                        )
                        revision == null -> eb.send(
                            TWCVERT_ADDRESS, JsonObject.mapFrom(
                                Message(
                                    GET_REVISIONS,
                                    JsonObject()
                                        .put(
                                            WORKSPACE_ID,
                                            workspaceId
                                        )
                                        .put(
                                            RESOURCE_ID,
                                            resourceId
                                        )
                                        .put(
                                            BRANCH_ID,
                                            branchId
                                        )
                                )
                            )
                        )
                        else -> vertx.eventBus().send(
                            TWCVERT_ADDRESS, JsonObject.mapFrom(
                                Message(
                                    GET_ROOT_ELEMENT_IDS,
                                    JsonObject()
                                        .put(
                                            WORKSPACE_ID,
                                            workspaceId
                                        )
                                        .put(
                                            RESOURCE_ID,
                                            resourceId
                                        )
                                        .put(
                                            BRANCH_ID,
                                            branchId
                                        )
                                        .put(
                                            REVISION_ID,
                                            revision
                                        )
                                )
                            )
                        )
                    }

                }
                REPO -> {
                    val repo = asJson(messageData.obj).mapTo(Repo::class.java)
                    val workspaceIds = repo.workspaces.map { ws ->
                        asJson(ws).getString("@id")
                    }

                    log(LogMessage.WorkspacesMessage(
                            workspaces = workspaceIds
                    ))

                    workspaceIds.forEach { id ->
                        eb.send(
                            TWCVERT_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    GET_RESOURCES,
                                    JsonObject().put(
                                        WORKSPACE_ID,
                                        id
                                    )
                                )
                            )
                        )
                    }
                }
                WORKSPACE -> {
                    val workspace = asJson(messageData.obj).mapTo(Workspace::class.java)
                    val workspaceId = workspace.id
                    val workspaceTitle = workspace.title

                    val workspaceLog = LogEntity(workspace.id, workspace.title)
                    val resourcesLog = workspace.resources.map { resource ->
                        val json = asJson(resource)
                        LogEntity(json.getString("ID"), json.getString("dcterms:title"))
                    }
                    log(LogMessage.ResourcesMessage(
                        workspace = workspaceLog, resources = resourcesLog
                    ))

                    workspace.resources.forEach { res ->
                        val resourceId = JsonObject(res as Map<String, Any>).getString("ID")
                        val resourceTitle = JsonObject(res as Map<String, Any>).getString("dcterms:title")
                        eb.send(
                            TWCVERT_ADDRESS, JsonObject.mapFrom(
                                Message(
                                    GET_BRANCHES,
                                    JsonObject()
                                        .put(
                                            WORKSPACE_ID,
                                            workspaceId
                                        ).put(
                                            WORKSPACE_TITLE,
                                            workspaceTitle
                                            )
                                        .put(
                                            RESOURCE_ID,
                                            resourceId
                                        ).put(
                                            RESOURCE_TITLE,
                                            resourceTitle
                                            )

                                )
                            )
                        )
                    }
                }
                RESOURCE -> {
                    val resource = asJson(messageData.obj).mapTo(Resource::class.java)
                    val resourceId = resource.id
                    val resourceTitle = resource.title
                    val workspaceId = resource.workspace_id
                    val workspaceTitle = resource.workspace_title

                    val brancheIds = resource.branches.map { branch ->
                        asJson(branch).getString("@id")
                    }
                    log(LogMessage.BranchesMessage(
                            workspace = LogEntity(workspaceId, workspaceTitle),
                            resource = LogEntity(resourceId, resourceTitle),
                            branches = brancheIds
                    ))

                    brancheIds.forEach { branchId ->
                        eb.send(
                            TWCVERT_ADDRESS, JsonObject.mapFrom(
                                Message(
                                    GET_REVISIONS,
                                    JsonObject()
                                        .put(
                                            WORKSPACE_ID,
                                            workspaceId
                                        ).put(
                                            WORKSPACE_TITLE,
                                            workspaceTitle
                                        ).put
                                        (
                                            RESOURCE_ID,
                                            resourceId
                                        ).put(
                                            RESOURCE_TITLE,
                                            resourceTitle
                                        ).put(
                                            BRANCH_ID,
                                            branchId
                                        )
                                )
                            )
                        )
                    }

                }
                BRANCH -> {
                    val branch = asJson(messageData.obj).mapTo(Branch::class.java)
                    val branchId = branch.id
                    val resourceId = branch.resource_id
                    val workspaceId = branch.workspace_id

                    val revisionsMessage = branch.revisions.map { revision ->
                        revision.toString()
                    }

                    log(LogMessage.RevisionsMessage(
                            workspace = LogEntity(branch.workspace_id, branch.workspace_title),
                            resource = LogEntity(branch.resource_id, branch.resource_title),
                            branch = LogEntity(branch.id, branch.title),
                            revisions = revisionsMessage
                    ))

                    branch.revisions.forEach { rev ->
                        val revId = (rev as Int)
                        val inputRevision = twcMap[REVISION]
                        if (revId == inputRevision) {
                            vertx.eventBus().send(
                                TWCVERT_ADDRESS, JsonObject.mapFrom(
                                    Message(
                                        GET_ROOT_ELEMENT_IDS,
                                        JsonObject()
                                            .put(
                                                WORKSPACE_ID,
                                                workspaceId
                                            )
                                            .put(
                                                RESOURCE_ID,
                                                resourceId
                                            )
                                            .put(
                                                BRANCH_ID,
                                                branchId
                                            )
                                            .put(
                                                REVISION_ID,
                                                revId
                                            )
                                    )
                                )
                            )
                        }
                    }

                }
                REVISION -> {
                    val revision = JsonObject(messageData.obj as Map<String, Any>).mapTo(Revision::class.java)
                    println("Received revision content for $revision")
                    val revisionId = revision.id
                    val branchId = revision.branch_id
                    val resourceId = revision.resource_id
                    val workspaceId = revision.workspace_id
                    val inputRevision = twcMap[REVISION]
                    if (revisionId == inputRevision) {
                        if (requestSingleElements) {
                            revision.elements.forEach { element ->
                                val elemId = element as String
                                vertx.eventBus().send(
                                    TWCVERT_ADDRESS, JsonObject.mapFrom(
                                        Message(
                                            GET_ELEMENT,
                                            JsonObject()
                                                .put(
                                                    WORKSPACE_ID,
                                                    workspaceId
                                                )
                                                .put(
                                                    RESOURCE_ID,
                                                    resourceId
                                                )
                                                .put(
                                                    BRANCH_ID,
                                                    branchId
                                                )
                                                .put(
                                                    REVISION_ID,
                                                    revisionId
                                                )
                                                .put(
                                                    ELEMENT_ID,
                                                    elemId
                                                )
                                        )
                                    )
                                )
                            }
                        } else {
                            val elementIds = revision.elements
                            vertx.eventBus().send(
                                TWCVERT_ADDRESS, JsonObject.mapFrom(
                                    Message(
                                        GET_ELEMENTS,
                                        JsonObject()
                                            .put(
                                                WORKSPACE_ID,
                                                workspaceId
                                            )
                                            .put(
                                                RESOURCE_ID,
                                                resourceId
                                            )
                                            .put(
                                                BRANCH_ID,
                                                branchId
                                            )
                                            .put(
                                                REVISION_ID,
                                                revisionId
                                            )
                                            .put(
                                                ELEMENT_IDS,
                                                elementIds
                                            )
                                    )
                                )
                            )
                        }
                    }

                }
                ELEMENT -> {

                    val elementRequest = JsonObject(messageData.obj as Map<String, Any>).mapTo(Element::class.java)
                    val revisionId = elementRequest.revision_id
                    val branchId = elementRequest.branch_id
                    val resourceId = elementRequest.resource_id
                    val workspaceId = elementRequest.workspace_id
                    elementRequest.elements.forEach { element ->
                        val elementId = JsonObject(element as Map<String, Any>).getString("@id")

                        vertx.eventBus().send(
                            TWCVERT_ADDRESS, JsonObject.mapFrom(
                                Message(
                                    GET_ELEMENT,
                                    JsonObject()
                                        .put(
                                            WORKSPACE_ID,
                                            workspaceId
                                        )
                                        .put(
                                            RESOURCE_ID,
                                            resourceId
                                        )
                                        .put(
                                            BRANCH_ID,
                                            branchId
                                        )
                                        .put(
                                            REVISION_ID,
                                            revisionId
                                        )
                                        .put(
                                            ELEMENT_ID,
                                            elementId
                                        )
                                )
                            )
                        )
                    }
                }
                ELEMENTS -> {
                    //                    println("Received Elements")
                    val elements = JsonObject(messageData.obj as Map<String, Any>).mapTo(Elements::class.java)
                    val revisionId = elements.revision_id
                    val branchId = elements.branch_id
                    val resourceId = elements.resource_id
                    val workspaceId = elements.workspace_id
                    val elementIds = elements.elements.map { element ->
                        JsonObject.mapFrom(element).getString("@id")
                    }
                    vertx.eventBus().send(
                        TWCVERT_ADDRESS, JsonObject.mapFrom(
                            Message(
                                GET_ELEMENTS,
                                JsonObject()
                                    .put(
                                        WORKSPACE_ID,
                                        workspaceId
                                    )
                                    .put(
                                        RESOURCE_ID,
                                        resourceId
                                    )
                                    .put(
                                        BRANCH_ID,
                                        branchId
                                    )
                                    .put(
                                        REVISION_ID,
                                        revisionId
                                    )
                                    .put(
                                        ELEMENT_IDS,
                                        elementIds
                                    )
                            )
                        )
                    )
                }
                ERROR -> {
                    println("\nExit")
                    if (twcMap["cookies"] != null) {
                        eb.send(
                            TWCVERT_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    LOGOUT,
                                    JsonObject()
                                )
                            )
                        )
                    }
                    vertx.close()
                }
                EXIT -> {
                    println("\nExit")
                    if (twcMap["cookies"] != null) {
                        eb.send(
                            TWCVERT_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    LOGOUT,
                                    JsonObject()
                                )
                            )
                        )
                    }
                    vertx.close()
                }

                else -> error("Unknown Command: ${messageData.event}")
            }

        }
    }
}