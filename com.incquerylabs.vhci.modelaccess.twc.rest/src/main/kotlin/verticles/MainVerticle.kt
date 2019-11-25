package com.incquerylabs.twc.repo.crawler.verticles

import com.incquerylabs.twc.repo.crawler.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject

class MainVerticle(val usr: String, val pswd: String) : AbstractVerticle() {
    var sum = 0
    var number = 0
    var s = 0
    var queries = 0

    override fun start() {
        val twcMap = vertx.sharedData().getLocalMap<Any, Any>(TWCMAP)
        val requestSingleElements = twcMap["requestSingleElement"] as Boolean

        val eb = vertx.eventBus()
        vertx.setPeriodic(1000) {
            vertx.sharedData().getCounter(QUERIES) { res ->
                if (res.succeeded()) {
                    val counter = res.result()
                    counter.get { res ->
                        queries = res.result().toInt()

                        vertx.sharedData().getCounter("sum") { res ->
                            if (res.succeeded()) {
                                val counter = res.result()
                                counter.get { get ->
                                    if (get.succeeded()) {
                                        sum = get.result().toInt()

                                        vertx.sharedData().getCounter("number") { res ->
                                            if (res.succeeded()) {
                                                val counter = res.result()
                                                counter.get { get ->
                                                    if (get.succeeded()) {
                                                        number = get.result().toInt()
                                                        println("Total: ${queries}/${sum}/${++s} query/response/sec | Now: $number elem | AvgSpeed: ${sum / (s)} elem/sec")
                                                        counter.compareAndSet(number.toLong(), 0, {})

                                                        if (queries != 0 && sum != 0 && queries == sum) {
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
                                                error("Counter: queries not available.")
                                            }
                                        }
                                    }
                                }
                            } else {
                                error("Counter: queries not available.")
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
                    User("$usr", "$pswd")
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
                    //                    println("Received Repository")
                    val repo = JsonObject(messageData.obj as Map<String, Any>).mapTo(Repo::class.java)
                    repo.workspaces.forEach { ws ->
                        val id = JsonObject(ws as Map<String, Any>).getString("@id")
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
                    //                    println("Received Workspace")
                    val workspace = JsonObject(messageData.obj as Map<String, Any>).mapTo(Workspace::class.java)
                    val workspaceId = workspace.id

                    workspace.resources.forEach { res ->
                        val resourceId = JsonObject(res as Map<String, Any>).getString("@id")
                        eb.send(
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
                    }
                }
                RESOURCE -> {
                    //                    println("Received Resource")
                    val resource = JsonObject(messageData.obj as Map<String, Any>).mapTo(Resource::class.java)
                    val resourceId = resource.id
                    val workspaceId = resource.workspace_id
                    resource.branches.forEach { branch ->
                        val branchId = JsonObject(branch as Map<String, Any>).getString("@id")
                        eb.send(
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
                    }

                }
                BRANCH -> {
                    //                    println("Received Branch")
                    val branch = JsonObject(messageData.obj as Map<String, Any>).mapTo(Branch::class.java)
                    val branchId = branch.id
                    val resourceId = branch.resource_id
                    val workspaceId = branch.workspace_id
                    val inputRevision = twcMap[REVISION]
                    if (inputRevision == null) {
                        twcMap[REVISION] = branch.revisions.maxBy { it as Int }
                    }

                    branch.revisions.forEach { rev ->
                        val revId = (rev as Int)
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
                REVISION -> {
                    //                    println("Received Revision")
                    val revision = JsonObject(messageData.obj as Map<String, Any>).mapTo(Revision::class.java)
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
                    //                    println("Received Element")
                    val element = JsonObject(messageData.obj as Map<String, Any>).mapTo(Element::class.java)
                    val revisionId = element.revision_id
                    val branchId = element.branch_id
                    val resourceId = element.resource_id
                    val workspaceId = element.workspace_id
                    element.elements.forEach { element ->
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