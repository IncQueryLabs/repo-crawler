package com.incquerylabs.vhci.modelaccess.twc.rest.verticles

import com.incquerylabs.vhci.modelaccess.twc.rest.data.Branch
import com.incquerylabs.vhci.modelaccess.twc.rest.data.DataConstants
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Element
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Elements
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Message
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Repo
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Resource
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Revision
import com.incquerylabs.vhci.modelaccess.twc.rest.data.Workspace
import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.LocalMap
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import java.io.File


class RESTVerticle() : AbstractVerticle() {

    var serverPath = ""
    var port = 8080
    var chunkSize = -1

    override fun start() {
        val twcMap = vertx.sharedData().getLocalMap<Any, Any>(DataConstants.TWCMAP)
        val client = WebClient.create(vertx, WebClientOptions().setSsl(twcMap["server_ssl"] as Boolean))

        serverPath = twcMap["server_path"].toString()
        port = twcMap["server_port"] as Int
        chunkSize = twcMap["chunkSize"] as Int

        val debug = twcMap["debug"] as Boolean

        vertx.eventBus().consumer<Any>(DataConstants.TWCVERT_ADDRESS) { message ->
            val json = message.body() as JsonObject
            val obj = json.getJsonObject("obj")

            when (json.getString("event")) {
                DataConstants.LOGIN -> {
                    println("Try to login. Username: ${json.getJsonObject("obj").getString("username")}")
                    //println(obj)
                    login(client, twcMap)
                }
                DataConstants.LOGOUT -> {
                    println("Log out")
                    logout(client, twcMap)
                }
                DataConstants.GET_WORKSPACES -> {
                    if (debug) {
                        println("Query Workspaces")
                        println(obj)
                    }
                    getWorkspaces(client, twcMap)
                }
                DataConstants.GET_RESOURCES -> {
                    if (debug) {
                        println("Query Resources")
                        println(obj)
                    }
                    getResources(client, twcMap, obj)
                }
                DataConstants.GET_BRANCHES -> {
                    if (debug) {
                        println("Query Branches")
                        println(obj)
                    }
                    getBranches(client, twcMap, obj)
                }
                DataConstants.GET_REVISIONS -> {
                    if (debug) {
                        println("Query Revisions")
                        println(obj)
                    }
                    getRevisions(client, twcMap, obj)
                }
                DataConstants.GET_ROOT_ELEMENT_IDS -> {
                    if (debug) {
                        println("Search Root Element Ids")
                        println(obj)
                    }
                    getRootElementIds(client, twcMap, obj)
                }
                DataConstants.GET_ELEMENT -> {
                    if (debug) {
                        println("Query Element")
                        println(obj)
                    }
                    getElement(client, twcMap, obj)
                }
                DataConstants.GET_ELEMENTS -> {
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
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revisionId = obj.getInteger(DataConstants.REVISION_ID)
        val elementIds = obj.getJsonArray(DataConstants.ELEMENT_IDS)
        val elementSize = elementIds.size().toLong()
        queryPrepared(elementSize)

        client.post(
            port, serverPath,
            "/osmc/workspaces/$workspaceId/resources/$resourceId/branches/$branchId/revisions/$revisionId/elements"
        )
            .putHeader("content-type", "text/plain")
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
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
                                    val elementM = Elements(revisionId, branchId, resourceId, workspaceId, chunkList)
                                    vertx.eventBus().send(
                                        DataConstants.TWCMAIN_ADDRESS,
                                        JsonObject.mapFrom(Message(DataConstants.ELEMENTS, elementM))
                                    )
                                }
                            } else {
                                val elementM =
                                    Elements(revisionId, branchId, resourceId, workspaceId, containedElements)
                                vertx.eventBus().send(
                                    DataConstants.TWCMAIN_ADDRESS,
                                    JsonObject.mapFrom(Message(DataConstants.ELEMENTS, elementM))
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
        vertx.sharedData().getCounter(DataConstants.QUERIES) { res ->
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
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revisionId = obj.getInteger(DataConstants.REVISION_ID)
        val elementId = obj.getString(DataConstants.ELEMENT_ID)
        client.get(
            port, serverPath,
            "/osmc/workspaces/$workspaceId/resources/$resourceId/branches/$branchId/elements/$elementId"
        )
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
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
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.ELEMENT, element))
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
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revId = obj.getInteger(DataConstants.REVISION_ID)
        client.get(
            port, serverPath,
            "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions/${revId}"
        )
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
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
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.REVISION, revision))
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
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        client.get(
            port, serverPath,
            "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions"
        )
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonArray()
                        val branch = Branch(
                            branchId,
                            resourceId,
                            workspaceId,
                            data.getJsonObject(0).getJsonArray("ldp:contains").list
                        )
                        //println(resource)
                        vertx.eventBus().send(
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.BRANCH, branch))
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
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        client.get(port, serverPath, "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches")
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonObject()
                        val resource = Resource(resourceId, workspaceId, data.getJsonArray("ldp:contains").list)
                        //println(resource)
                        vertx.eventBus().send(
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.RESOURCE, resource))
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
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        client.get(port, serverPath, "/osmc/workspaces/${workspaceId}/resources")
            .putHeader("content-type", "application/ld+json")
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonArray()
                        val workspace = Workspace(workspaceId, data.getJsonObject(0).getJsonArray("ldp:contains").list)
                        //println(workspace)
                        vertx.eventBus().send(
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.WORKSPACE, workspace))
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
                        twcMap[DataConstants.USER] = userCookie
                        twcMap[DataConstants.SESSION] = sessionCookie
                        val currentTimeMillis = System.currentTimeMillis()
                        val sessionFile = "session_details_$currentTimeMillis"
                        twcMap["sessionFile"] = sessionFile
                        File(sessionFile).writeText("""
                            user: $userCookie
                            session: $sessionCookie
                        """.trimIndent())
                        println("Session details written to: $sessionFile")
                        vertx.eventBus().send(
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.LOGGED_IN, JsonObject()))
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
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 204) {
                        twcMap.remove("cookies")
                        val sessionFile = twcMap["sessionFile"] as String
                        println("Logout successful, deleting session file $sessionFile")
                        File(sessionFile).deleteOnExit()
                        vertx.eventBus().send(
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.EXIT, JsonObject()))
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
            .putHeader("Cookie", "${twcMap[DataConstants.USER]}")
            .putHeader("Cookie", "${twcMap[DataConstants.SESSION]}")
            .timeout(2000)
            .send { ar ->
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {

                        val data = ar.result().bodyAsJsonObject()
                        val repoID = data.getString("@id")
                        val workspaces = data.getJsonArray("ldp:contains")

                        val repo = Repo(repoID, workspaces.list)

                        vertx.eventBus().send(
                            DataConstants.TWCMAIN_ADDRESS,
                            JsonObject.mapFrom(Message(DataConstants.REPO, JsonObject.mapFrom(repo)))
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
            .send(DataConstants.TWCMAIN_ADDRESS, JsonObject.mapFrom(Message(DataConstants.ERROR, JsonObject())))
    }

}