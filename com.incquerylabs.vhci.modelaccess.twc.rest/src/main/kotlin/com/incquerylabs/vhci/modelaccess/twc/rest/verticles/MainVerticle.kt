package com.incquerylabs.vhci.modelaccess.twc.rest.verticles

import com.incquerylabs.vhci.modelaccess.twc.rest.data.Message
import com.incquerylabs.vhci.modelaccess.twc.rest.data.User
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject

var sum = 0
var number = 0
var s = 0
var queries = 0

class MainVerticle(val usr:String, val pswd:String) : AbstractVerticle(){

    override fun start() {
        val twcMap = vertx.sharedData().getLocalMap<Any,Any>("twcMap")
        val eb = vertx.eventBus()
        vertx.setPeriodic(1000,{
            vertx.sharedData().getCounter("queries", { res ->
                if (res.succeeded()) {
                    val counter = res.result()
                    counter.get { res ->
                        queries = res.result().toInt()

                        vertx.sharedData().getCounter("sum", { res ->
                            if (res.succeeded()) {
                                val counter = res.result()
                                counter.get { get ->
                                    if(get.succeeded()){
                                        sum = get.result().toInt()

                                        vertx.sharedData().getCounter("number", { res ->
                                            if (res.succeeded()) {
                                                val counter = res.result()
                                                counter.get { get ->
                                                    if(get.succeeded()){
                                                        number = get.result().toInt()
                                                        println("Total: ${queries}/${sum}/${++s} query/response/sec | Now: ${number} elem | AvgSpeed: ${sum/(s)} elem/sec")
                                                        counter.compareAndSet(number.toLong(),0,{})

                                                        if(queries!=0 && sum!=0 && queries==sum){
                                                            eb.send("twc.rest.twcvert",Json.encode(Message("logout",JsonObject())))
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
        eb.send("twc.rest.twcvert", Json.encode(Message("login", User("$usr", "$pswd"))))
        eb.consumer<Any>("twc.rest.main", { message ->

            val json = JsonObject(message.body().toString())
            val data = json.getJsonObject("obj")

            when (json.getString("event")) {
                "logged_in" -> {
                    println("Login complete")

                    eb.send("twc.rest.twcvert", Json.encode(Message("getWorkspaces", JsonObject())))
                    //eb.send("twc.rest.twcvert", Json.encode(Message("logout", JsonObject())))
                }
                "repo" -> {
//                    println("Recieved Repository")
                    val repo = JsonObject(twcMap.get("repo") as String)
                    repo.getJsonArray("workspaces").forEach { ws ->
                        val id = (ws as JsonObject).getString("@id")
                        eb.send("twc.rest.twcvert", Json.encode(Message("getResources", JsonObject().put("workspace_id", id))))
                    }
                }
                "workspace" -> {
//                    println("Recieved Workspace")
//                    println(data)
                    val workspaceId = data.getString("id")
                    data.getJsonArray("resources").forEach { res ->
                        val resourceId = (res as JsonObject).getString("@id")
                        eb.send("twc.rest.twcvert", Json.encode(Message("getBranches",
                                JsonObject()
                                        .put("workspace_id", workspaceId)
                                        .put("resource_id", resourceId))))
                    }
                }
                "resource" -> {
//                    println("Recieved Resource")
//                    println(data)

                    val resourceId = data.getString("id")
                    val workspaceId = data.getString("workspace_id")
                    data.getJsonArray("branches").forEach { branch ->
                        val branchId = (branch as JsonObject).getString("@id")
                        eb.send("twc.rest.twcvert", Json.encode(Message("getRevisions",
                                JsonObject()
                                        .put("workspace_id", workspaceId)
                                        .put("resource_id", resourceId)
                                        .put("branch_id", branchId)
                        )))
                    }

                }
                "branch" -> {
//                    println("Recieved Branch")
//                    println(data)

                    val branchId = data.getString("id")
                    val resourceId = data.getString("resource_id")
                    val workspaceId = data.getString("workspace_id")
                    data.getJsonArray("revisions").forEach { rev ->
                        val revId = (rev as Int)
                        vertx.eventBus().send("twc.rest.twcvert", Json.encode(Message("getRootElementIds",
                                JsonObject()
                                        .put("workspace_id", workspaceId)
                                        .put("resource_id", resourceId)
                                        .put("branch_id", branchId)
                                        .put("rev_id", revId)
                        )))
                    }

                }
                "revision" -> {
//                    println("Recieved Revision")
//                    println(data)

                    val branchId = data.getString("branch_id")
                    val resourceId = data.getString("resource_id")
                    val workspaceId = data.getString("workspace_id")
                    data.getJsonArray("elements").forEach { element ->
                        val elemId = element as String
                        vertx.eventBus().send("twc.rest.twcvert", Json.encode(Message("getRootElement",
                                JsonObject()
                                        .put("workspace_id", workspaceId)
                                        .put("resource_id", resourceId)
                                        .put("branch_id", branchId)
                                        .put("element_id", elemId)
                        )))
                    }

                }
                "element" -> {
//                    println("Recieved Element")
//                    println(data)

                    val branchId = data.getString("branch_id")
                    val resourceId = data.getString("resource_id")
                    val workspaceId = data.getString("workspace_id")
                    data.getJsonArray("elements").forEach { element ->
                        val element_id = (element as JsonObject).getString("@id")

                        vertx.eventBus().send("twc.rest.twcvert", Json.encode(Message("getElement",
                                JsonObject()
                                        .put("workspace_id", workspaceId)
                                        .put("resource_id", resourceId)
                                        .put("branch_id", branchId)
                                        .put("element_id", element_id)
                        )))
                    }
                }
                "error" -> {
                    println("\nExit")
                    vertx.close()
                }
                "exit" -> {
                    println("\nExit")
                    vertx.close()
                }

                else -> error("Unknown Command: ${json.getString("event")}")
            }

        })
    }
}