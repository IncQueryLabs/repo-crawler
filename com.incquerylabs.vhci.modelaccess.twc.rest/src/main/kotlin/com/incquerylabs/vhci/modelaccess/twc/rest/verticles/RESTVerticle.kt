package com.incquerylabs.vhci.modelaccess.twc.rest.verticles

import com.incquerylabs.vhci.modelaccess.twc.rest.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.LocalMap
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions


class RESTVerticle() : AbstractVerticle() {

    var serverPath = ""
    var port = 8080
    var chunkSize = -1

    override fun start() {
        val client = WebClient.create(vertx, WebClientOptions())
        val twcMap = vertx.sharedData().getLocalMap<Any, Any>(DataConstants.TWCMAP)

        serverPath = twcMap["server_path"].toString()
        port = twcMap["server_port"] as Int
        chunkSize = twcMap["chunkSize"] as Int

        val debug = twcMap["debug"] as Boolean

        vertx.eventBus().consumer<Any>(DataConstants.TWCVERT_ADDRESS, { message ->
            val json = JsonObject(message.body().toString())
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

        })


    }

    private fun getElements(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revisionId = obj.getInteger(DataConstants.REVISION_ID)
        val elementIds = obj.getJsonArray(DataConstants.ELEMENT_IDS)
        vertx.sharedData().getCounter(DataConstants.QUERIES, { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.addAndGet(elementIds.size().toLong(), {})
            } else {
                error("Counter: queries not available.")
            }
        })

        client.post(port, serverPath,
                "/osmc/workspaces/$workspaceId/resources/$resourceId/branches/$branchId/revisions/$revisionId/elements")
                .putHeader("content-type", "text/plain")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap["user_cookie"]}")
                .putHeader("Cookie", "${twcMap["session_cookie"]}")
                .sendBuffer(Buffer.buffer(elementIds.joinToString(",")), { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonObject()

                            val containedElements = elementIds.flatMap { elementId ->
                                val element = data.getJsonArray(elementId as String)
                                saveElement(elementId, element)
                                element.getJsonObject(0).getJsonArray("ldp:contains")
                            }
                            if (!containedElements.isEmpty()) {
                                if (chunkSize > 1) {
                                    containedElements.withIndex().groupBy {
                                        it.index / chunkSize
                                    }.values.map { it.map { it.value } }.forEach { chunkList ->
                                        val elementM = Elements(revisionId, branchId, resourceId, workspaceId, JsonArray(chunkList))
                                        vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.ELEMENTS, elementM)))
                                    }
                                } else {
                                    val elementM = Elements(revisionId, branchId, resourceId, workspaceId, JsonArray(containedElements))
                                    vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.ELEMENTS, elementM)))
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

                })
    }

    private fun getElement(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        getRootElement(client, twcMap, obj)
    }

    private fun getRootElement(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        vertx.sharedData().getCounter(DataConstants.QUERIES, { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.incrementAndGet {}
            } else {
                error("Counter: queries not available.")
            }
        })
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revisionId = obj.getInteger(DataConstants.REVISION_ID)
        val elementId = obj.getString(DataConstants.ELEMENT_ID)
        client.get(port, serverPath,
                "/osmc/workspaces/$workspaceId/resources/$resourceId/branches/$branchId/elements/$elementId")
                .putHeader("content-type", "application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .sendJson(JsonObject(), { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()

                            saveElement(elementId, data)

                            val element = Element(elementId, revisionId, branchId, resourceId, workspaceId,
                                    data.getJsonObject(0).getJsonArray("ldp:contains"))

                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.ELEMENT, element)))

                        } else {
                            println("getRootElement: ${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                            myError()
                        }
                    } else {
                        println("Query Root Element failed: ${ar.cause().message}")
                        myError()
                    }

                })
    }

    private fun saveElement(elementId: String, data: JsonArray) {
        vertx.sharedData().getCounter("sum", { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.incrementAndGet {}
            } else {
                error("Counter: queries not available.")
            }
        })
        vertx.sharedData().getCounter("number", { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.incrementAndGet {}
            } else {
                error("Counter: queries not available.")
            }
        })
//        vertx.executeBlocking<Any>({ future ->
//            val dir = File("./elements")
//            val file = File("./elements/${elementId}.json")
//            if(!dir.isDirectory || !dir.exists()){
//                dir.mkdir()
//            }
//            if(!file.exists()){
//                file.createNewFile()
//            }
//            file.writeText(data.toString())
//
//            future.complete("${elementId}.json")
//        },{ res ->
//            println("${res.result()} saved")
//        })
    }

    private fun getRootElementIds(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revId = obj.getInteger(DataConstants.REVISION_ID)
        client.get(port, serverPath,
                "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions/${revId}")
                .putHeader("content-type", "application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .send { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()
                            //println(data)
                            val revision = Revision(revId, branchId, resourceId, workspaceId,
                                    data.getJsonObject(0).getJsonArray("rootObjectIDs"))
                            //println(revision)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.REVISION, revision)))

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
        client.get(port, serverPath,
                "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions")
                .putHeader("content-type", "application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .send { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()
                            val branch = Branch(branchId, resourceId, workspaceId, data.getJsonObject(0).getJsonArray("ldp:contains"))
                            //println(resource)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.BRANCH, branch)))

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
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .send { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonObject()
                            val resource = Resource(resourceId, workspaceId, data.getJsonArray("ldp:contains"))
                            //println(resource)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.RESOURCE, resource)))

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
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .send { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()
                            val workspace = Workspace(workspaceId, data.getJsonObject(0).getJsonArray("ldp:contains"))
                            //println(workspace)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.WORKSPACE, workspace)))

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
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .send({ ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 204) {
                            val userCookie = ar.result().cookies()[0].split(';')[0]
                            val sessionCookie = ar.result().cookies()[1].split(';')[0]
                            twcMap.put("user_cookie", userCookie)
                            twcMap.put("session_cookie", sessionCookie)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.LOGGED_IN, JsonObject())))
                        } else {
                            println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                            myError()
                        }

                    } else {
                        ar.cause().printStackTrace()
                        println("Login failed: ${ar.cause().message}")
                        myError()
                    }
                })
    }

    private fun logout(client: WebClient, twcMap: LocalMap<Any, Any>) {
        client.get(port, serverPath, "/osmc/logout")
                .putHeader("content-type", "application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .send { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 204) {
                            //twcMap.put("cookies",ar.result().cookies().toString())
                            twcMap.remove("cookies")
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.EXIT, JsonObject())))
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

    //TODO
    private fun getWorkspaces(client: WebClient, twcMap: LocalMap<Any, Any>) {
        client.get(port, serverPath, "/osmc/workspaces")
                .putHeader("content-type", "application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie", "${twcMap.get("user_cookie")}")
                .putHeader("Cookie", "${twcMap.get("session_cookie")}")
                .timeout(2000)
                .send { ar ->
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonObject()
                            val repoID = data.getString("@id")
                            val workspaces = data.getJsonArray("ldp:contains")

                            val repo = Repo(repoID, workspaces)

                            //println(repo)
                            twcMap.put("repo", Json.encode(repo))

                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.REPO, JsonObject())))
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
        vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message(DataConstants.ERROR, JsonObject())))
    }

}