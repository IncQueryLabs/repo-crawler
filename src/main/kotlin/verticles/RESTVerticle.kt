package com.incquerylabs.twc.repo.crawler.verticles

import com.incquerylabs.twc.repo.crawler.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.LocalMap
import io.vertx.ext.web.client.WebClient
import java.io.File


class RESTVerticle(
    val configuration: CrawlerConfiguration
) : AbstractVerticle() {

    val serverPath = configuration.server.path
    val port = configuration.server.port
    val chunkSize = configuration.chunkSize

    override fun start() {
        val twcMap = vertx.sharedData().getLocalMap<Any, Any>(TWCMAP)
        val client = WebClient.create(vertx, configuration.webClientOptions)

        val debug = configuration.debug

        vertx.eventBus().consumer<Any>(TWCVERT_ADDRESS) { message ->
            val json = message.body() as JsonObject
            val obj = json.getJsonObject("obj")

            when (json.getString("event")) {
                LOGIN -> {
                    println("Try to login. Username: ${json.getJsonObject("obj").getString("username")}")
                    //println(obj)
                    login(client, twcMap)
                }
                LOGOUT -> {
                    println("Log out")
                    logout(client, twcMap)
                }
                GET_WORKSPACES -> {
                    if (debug) {
                        println("Query Workspaces")
                        println(obj)
                    }
                    getWorkspaces(client, twcMap)
                }
                GET_RESOURCES -> {
                    if (debug) {
                        println("Query Resources")
                        println(obj)
                    }
                    getResources(client, twcMap, obj)
                }
                GET_BRANCHES -> {
                    if (debug) {
                        println("Query Branches")
                        println(obj)
                    }
                    getBranches(client, twcMap, obj)
                }
                GET_REVISIONS -> {
                    if (debug) {
                        println("Query Revisions")
                        println(obj)
                    }
                    getRevisions(client, twcMap, obj)
                }
                GET_ROOT_ELEMENT_IDS -> {
                    if (debug) {
                        println("Search Root Element Ids")
                        println(obj)
                    }
                    getRootElementIds(client, twcMap, obj)
                }
                GET_ELEMENT -> {
                    if (debug) {
                        println("Query Element")
                        println(obj)
                    }
                    getElement(client, twcMap, obj)
                }
                GET_ELEMENTS -> {
                    if (debug) {
                        println("Query Elements")
                        println(obj)
                    }
                    getElements(client, twcMap, obj)
                }
                else -> error("Unknown Command: ${json.getString("event")}")
            }

        }


    }

    private fun getElements(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(WORKSPACE_ID)
        val resourceId = obj.getString(RESOURCE_ID)
        val branchId = obj.getString(BRANCH_ID)
        val revisionId = obj.getInteger(REVISION_ID)
        val elementIds = obj.getJsonArray(ELEMENT_IDS)
        val elementSize = elementIds.size().toLong()
        queryPrepared(elementSize)

        client.post(
            port, serverPath,
            "/osmc/workspaces/$workspaceId/resources/$resourceId/branches/$branchId/revisions/$revisionId/elements"
        )
            .putHeader("content-type", "text/plain")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .sendBuffer(Buffer.buffer(elementIds.joinToString(","))) { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonObject()

                        queryCompleted(elementSize)
                        val containedElements = elementIds.flatMap { elementId ->
                            val element = data.getJsonObject(elementId as String).getJsonArray("data")
                            element.getJsonObject(0).getJsonArray("ldp:contains")
                        }
                        if (containedElements.isNotEmpty()) {
                            if (chunkSize > 1) {
                                containedElements.withIndex().groupBy {
                                    it.index / chunkSize
                                }.values.map { it.map { it.value } }.forEach { chunkList ->
                                    val elementM = Elements(
                                        revisionId,
                                        branchId,
                                        resourceId,
                                        workspaceId,
                                        chunkList
                                    )
                                    vertx.eventBus().send(
                                        TWCMAIN_ADDRESS,
                                        JsonObject.mapFrom(
                                            Message(
                                                ELEMENTS,
                                                elementM
                                            )
                                        )
                                    )
                                }
                            } else {
                                val elementM =
                                    Elements(
                                        revisionId,
                                        branchId,
                                        resourceId,
                                        workspaceId,
                                        containedElements
                                    )
                                vertx.eventBus().send(
                                    TWCMAIN_ADDRESS,
                                    JsonObject.mapFrom(
                                        Message(
                                            ELEMENTS,
                                            elementM
                                        )
                                    )
                                )
                            }
                        }

                    } else {
                        println("getElements: ${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Root Element failed: ${ar.cause().message}")
                    myError()
                }

            }

    }

    private fun queryPrepared(elementSize: Long) {
        vertx.sharedData().getCounter(QUERIES) { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.addAndGet(elementSize) {}
            } else {
                error("Counter: queries not available.")
            }
        }
    }

    private fun queryCompleted(elementSize: Long) {
        vertx.sharedData().getCounter("sum") { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.addAndGet(elementSize) {}
            } else {
                error("Counter: sum not available.")
            }
        }
        vertx.sharedData().getCounter("number") { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.addAndGet(elementSize) {}
            } else {
                error("Counter: number not available.")
            }
        }
    }

    private fun getElement(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        getRootElement(client, twcMap, obj)
    }

    private fun getRootElement(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        queryPrepared(1)
        val workspaceId = obj.getString(WORKSPACE_ID)
        val resourceId = obj.getString(RESOURCE_ID)
        val branchId = obj.getString(BRANCH_ID)
        val revisionId = obj.getInteger(REVISION_ID)
        val elementId = obj.getString(ELEMENT_ID)
        client.get(
            port, serverPath,
            "/osmc/workspaces/$workspaceId/resources/$resourceId/branches/$branchId/elements/$elementId"
        )
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .sendJson(JsonObject()) { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonArray()

                        queryCompleted(1)

                        val element = Element(
                            elementId, revisionId, branchId, resourceId, workspaceId,
                            data.getJsonObject(0).getJsonArray("ldp:contains").list
                        )

                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    ELEMENT,
                                    element
                                )
                            )
                        )

                    } else {
                        println("getRootElement: ${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Root Element failed: ${ar.cause().message}")
                    myError()
                }

            }
    }

    private fun getRootElementIds(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(WORKSPACE_ID)
        val resourceId = obj.getString(RESOURCE_ID)
        val branchId = obj.getString(BRANCH_ID)
        val revId = obj.getInteger(REVISION_ID)
        client.get(
            port, serverPath,
            "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions/${revId}"
        )
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonArray()
                        //println(data)
                        val revision = Revision(
                            revId, branchId, resourceId, workspaceId,
                            data.getJsonObject(0).getJsonArray("rootObjectIDs").list
                        )
                        //println(revision)
                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    REVISION,
                                    revision
                                )
                            )
                        )

                    } else {
                        println("getRootElementIds: ${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Root Element IDs failed: ${ar.cause().message}")
                    myError()
                }

            }
    }

    private fun getRevisions(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        queryPrepared(1)
        val workspaceId = obj.getString(WORKSPACE_ID)
        val workspaceTitle = obj.getString(WORKSPACE_TITLE) ?: ""
        val resourceId = obj.getString(RESOURCE_ID)
        val resourceTitle = obj.getString(RESOURCE_TITLE) ?: ""
        val branchId = obj.getString(BRANCH_ID)
        client.get(
            port, serverPath,
            "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}"
        )
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {
                        queryCompleted(1)
                        val data = ar.result().bodyAsJsonArray()
                        val branch = Branch(
                            branchId,
                            data.getJsonObject(1).getString("dcterms:title"),
                            resourceId,
                            resourceTitle,
                            workspaceId,
                            workspaceTitle,
                            data.getJsonObject(0).getJsonArray("ldp:contains").list
                        )
                        //println(resource)
                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    BRANCH,
                                    branch
                                )
                            )
                        )

                    } else {
                        println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Revisions failed: ${ar.cause().message}")
                    myError()
                }

            }
    }



    private fun getBranches(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(WORKSPACE_ID)
        val resourceId = obj.getString(RESOURCE_ID)
        val workspaceTitle = obj.getString(WORKSPACE_TITLE) ?: ""
        val resourceTitle = obj.getString(RESOURCE_TITLE) ?: ""
        client.get(port, serverPath, "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches")
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {
                        val data = ar.result().bodyAsJsonObject()
                        val resource = Resource(
                            resourceId,
                            resourceTitle,
                            workspaceId,
                            workspaceTitle,
                            data.getJsonArray("ldp:contains").list
                        )
                        //println(resource)
                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    RESOURCE,
                                    resource
                                )
                            )
                        )

                    } else {
                        println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Branches failed: ${ar.cause().message}")
                    myError()
                }

            }
    }

    private fun getResources(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(WORKSPACE_ID)
        client.get(port, serverPath, "/osmc/workspaces/${workspaceId}/resources?includeBody=True")
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonArray()
                        val resourceData = data.getJsonObject(1).getJsonArray("kerml:resources").list

                        val resources = resourceData.map {
                            it as HashMap<String,Any>
                            it.filterKeys { key -> key=="dcterms:title" || key=="ID" }
                        }

                        val workspace = Workspace(
                            workspaceId,
                            data.getJsonObject(1).getString("dcterms:title"),
                            resources
                        )
                        //println(workspace)
                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    WORKSPACE,
                                    workspace
                                )
                            )
                        )

                    } else {
                        println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Resources failed: ${ar.cause().message}")
                    myError()
                }
            }
    }

    private fun login(client: WebClient, twcMap: LocalMap<Any, Any>) {
        client.get(port, serverPath, "/osmc/login")
            .putHeader("content-type", "application/json")
            .putHeader("Accept", "text/html")
            .putHeader("Authorization", "${twcMap["credential"]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 204) {
                        val userCookie = ar.result().cookies()[0].split(';')[0]
                        val sessionCookie = ar.result().cookies()[1].split(';')[0]
                        twcMap[USER] = userCookie
                        twcMap[SESSION] = sessionCookie
                        val currentTimeMillis = System.currentTimeMillis()
                        val sessionFile = "session_details_$currentTimeMillis"
                        twcMap["sessionFile"] = sessionFile
                        File(sessionFile).writeText("""
                            user: $userCookie
                            session: $sessionCookie
                        """.trimIndent())
                        println("Session details written to: $sessionFile")
                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    LOGGED_IN,
                                    JsonObject()
                                )
                            )
                        )
                    } else {
                        println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }

                } else {
                    ar.cause().printStackTrace()
                    println("Login failed: ${ar.cause().message}")
                    myError()
                }
            }
    }

    private fun logout(client: WebClient, twcMap: LocalMap<Any, Any>) {
        client.get(port, serverPath, "/osmc/logout")
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 204) {
                        twcMap.remove("cookies")
                        val sessionFile = twcMap["sessionFile"] as String
                        println("Logout successful, deleting session file $sessionFile")
                        File(sessionFile).deleteOnExit()
                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    EXIT,
                                    JsonObject()
                                )
                            )
                        )
                    } else {
                        println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }

                } else {
                    ar.cause().printStackTrace()
                    println("Logout failed: ${ar.cause().message}")
                    myError()
                }
            }
    }

    private fun getWorkspaces(client: WebClient, twcMap: LocalMap<Any, Any>) {
        client.get(port, serverPath, "/osmc/workspaces")
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[USER]}")
            .putHeader("Cookie", "${twcMap[SESSION]}")
            .timeout(2000)
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonObject()
                        val repoID = data.getString("@id")
                        val workspaces = data.getJsonArray("ldp:contains")

                        val repo =
                            Repo(repoID, workspaces.list)

                        vertx.eventBus().send(
                            TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(
                                Message(
                                    REPO,
                                    JsonObject.mapFrom(repo)
                                )
                            )
                        )
                    } else {
                        println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                        myError()
                    }
                } else {
                    println("Query Workspaces failed: ${ar.cause().message}")
                    myError()
                }

            }
    }

    private fun myError() {
        vertx.eventBus()
            .send(
                TWCMAIN_ADDRESS, JsonObject.mapFrom(
                    Message(
                        ERROR,
                        JsonObject()
                    )
                ))
    }

}