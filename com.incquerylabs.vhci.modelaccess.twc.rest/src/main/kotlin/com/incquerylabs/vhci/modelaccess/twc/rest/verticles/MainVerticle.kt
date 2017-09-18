package com.incquerylabs.vhci.modelaccess.twc.rest.verticles

import com.incquerylabs.vhci.modelaccess.twc.rest.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject

class MainVerticle(val usr: String, val pswd: String) : AbstractVerticle() {
    var sum = 0
    var number = 0
    var s = 0
    var queries = 0

    override fun start() {
        val twcMap = vertx.sharedData().getLocalMap<Any, Any>(DataConstants.TWCMAP)
        val eb = vertx.eventBus()
        vertx.setPeriodic(1000, {
            vertx.sharedData().getCounter(DataConstants.QUERIES, { res ->
                if (res.succeeded()) {
                    val counter = res.result()
                    counter.get { res ->
                        queries = res.result().toInt()

                        vertx.sharedData().getCounter("sum", { res ->
                            if (res.succeeded()) {
                                val counter = res.result()
                                counter.get { get ->
                                    if (get.succeeded()) {
                                        sum = get.result().toInt()

                                        vertx.sharedData().getCounter("number", { res ->
                                            if (res.succeeded()) {
                                                val counter = res.result()
                                                counter.get { get ->
                                                    if (get.succeeded()) {
                                                        number = get.result().toInt()
                                                        println("Total: ${queries}/${sum}/${++s} query/response/sec | Now: ${number} elem | AvgSpeed: ${sum / (s)} elem/sec")
                                                        counter.compareAndSet(number.toLong(), 0, {})

                                                        if (queries != 0 && sum != 0 && queries == sum) {
                                                            eb.send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("logout", JsonObject())))
                                                        }
                                                    }
                                                }
                                            } else {
                                                error("Counter: queries not available.")
                                            }
                                        })
                                    }
                                }
                            } else {
                                error("Counter: queries not available.")
                            }
                        })


                    }
                } else {
                    // Something went wrong!
                }
            })

        })
        eb.send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("login", User("$usr", "$pswd"))))
        eb.consumer<Any>(DataConstants.TWCMAIN_ADDRESS, { message ->

            val json = JsonObject(message.body().toString())
            val data = json.getJsonObject("obj")

            when (json.getString("event")) {
                "logged_in" -> {
                    println("Login complete")

                    eb.send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getWorkspaces", JsonObject())))
                    //eb.send("twc.rest.twcvert", Json.encode(Message("logout", JsonObject())))
                }
                "repo" -> {
//                    println("Received Repository")
                    val repo = JsonObject(twcMap.get("repo") as String)
                    repo.getJsonArray("workspaces").forEach { ws ->
                        val id = (ws as JsonObject).getString("@id")
                        eb.send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getResources", JsonObject().put(DataConstants.WORKSPACE_ID, id))))
                    }
                }
                "workspace" -> {
//                    println("Received Workspace")
//                    println(data)
                    val workspaceId = data.getString("id")
                    data.getJsonArray("resources").forEach { res ->
                        val resourceId = (res as JsonObject).getString("@id")
                        eb.send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getBranches",
                                JsonObject()
                                        .put(DataConstants.WORKSPACE_ID, workspaceId)
                                        .put(DataConstants.RESOURCE_ID, resourceId))))
                    }
                }
                "resource" -> {
//                    println("Received Resource")
//                    println(data)

                    val resourceId = data.getString("id")
                    val workspaceId = data.getString(DataConstants.WORKSPACE_ID)
                    data.getJsonArray("branches").forEach { branch ->
                        val branchId = (branch as JsonObject).getString("@id")
                        eb.send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getRevisions",
                                JsonObject()
                                        .put(DataConstants.WORKSPACE_ID, workspaceId)
                                        .put(DataConstants.RESOURCE_ID, resourceId)
                                        .put(DataConstants.BRANCH_ID, branchId)
                        )))
                    }

                }
                "branch" -> {
//                    println("Received Branch")
//                    println(data)

                    val branchId = data.getString("id")
                    val resourceId = data.getString(DataConstants.RESOURCE_ID)
                    val workspaceId = data.getString(DataConstants.WORKSPACE_ID)
                    data.getJsonArray("revisions").forEach { rev ->
                        val revId = (rev as Int)
                        vertx.eventBus().send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getRootElementIds",
                                JsonObject()
                                        .put(DataConstants.WORKSPACE_ID, workspaceId)
                                        .put(DataConstants.RESOURCE_ID, resourceId)
                                        .put(DataConstants.BRANCH_ID, branchId)
                                        .put("rev_id", revId)
                        )))
                    }

                }
                "revision" -> {
//                    println("Received Revision")
//                    println(data)

                    val branchId = data.getString(DataConstants.BRANCH_ID)
                    val resourceId = data.getString(DataConstants.RESOURCE_ID)
                    val workspaceId = data.getString(DataConstants.WORKSPACE_ID)
                    data.getJsonArray(DataConstants.ELEMENTS).forEach { element ->
                        val elemId = element as String
                        vertx.eventBus().send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getRootElement",
                                JsonObject()
                                        .put(DataConstants.WORKSPACE_ID, workspaceId)
                                        .put(DataConstants.RESOURCE_ID, resourceId)
                                        .put(DataConstants.BRANCH_ID, branchId)
                                        .put(DataConstants.ELEMENT_ID, elemId)
                        )))
                    }

                }
                "element" -> {
//                    println("Received Element")
//                    println(data)

                    val branchId = data.getString(DataConstants.BRANCH_ID)
                    val resourceId = data.getString(DataConstants.RESOURCE_ID)
                    val workspaceId = data.getString(DataConstants.WORKSPACE_ID)
                    // TODO send IDs as a single message
                    data.getJsonArray(DataConstants.ELEMENTS).forEach { element ->
                        val element_id = (element as JsonObject).getString("@id")

                        vertx.eventBus().send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getElement",
                                JsonObject()
                                        .put(DataConstants.WORKSPACE_ID, workspaceId)
                                        .put(DataConstants.RESOURCE_ID, resourceId)
                                        .put(DataConstants.BRANCH_ID, branchId)
                                        .put(DataConstants.ELEMENT_ID, element_id)
                        )))
                    }
                }
                DataConstants.ELEMENTS -> {
//                    println("Received Elements")
//                    println(data)

                    val branchId = data.getString(DataConstants.BRANCH_ID)
                    val resourceId = data.getString(DataConstants.RESOURCE_ID)
                    val workspaceId = data.getString(DataConstants.WORKSPACE_ID)
                    // TODO send IDs as a single message
                    data.getJsonArray("element_ids").forEach { element ->
                        val element_id = (element as JsonObject).getString("@id")

                        vertx.eventBus().send(DataConstants.TWCVERT_ADDRESS, Json.encode(Message("getElement",
                                JsonObject()
                                        .put(DataConstants.WORKSPACE_ID, workspaceId)
                                        .put(DataConstants.RESOURCE_ID, resourceId)
                                        .put(DataConstants.BRANCH_ID, branchId)
                                        .put(DataConstants.ELEMENT_ID, element_id)
                        )))
                    }
                }
                DataConstants.ERROR -> {
                    println("\nExit")
                    vertx.close()
                }
                DataConstants.EXIT -> {
                    println("\nExit")
                    vertx.close()
                }

                else -> error("Unknown Command: ${json.getString("event")}")
            }

        })
    }
}