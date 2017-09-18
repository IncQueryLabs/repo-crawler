package com.incquerylabs.vhci.modelaccess.twc.rest.verticles

import com.incquerylabs.vhci.modelaccess.twc.rest.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.LocalMap
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.Cookie
import io.vertx.ext.web.client.WebClientOptions
import java.io.File
import java.net.CookieHandler

class RESTVerticle() : AbstractVerticle(){

    var serverPath = ""
    var port = 8080

    override fun start() {
        val client = WebClient.create(vertx,WebClientOptions())
        val twcMap = vertx.sharedData().getLocalMap<Any,Any>(DataConstants.TWCMAP)

        serverPath = twcMap.get("server_path").toString()
        port =  twcMap.get("server_port") as Int

        vertx.eventBus().consumer<Any>(DataConstants.TWCVERT_ADDRESS,{ message ->
            val json = JsonObject(message.body().toString())
            val obj = json.getJsonObject("obj")

            when(json.getString("event")){
                "login"->{
                    println("Try to login. Username: ${json.getJsonObject("obj").getString("username")}")
                    //println(obj)
                    login(client,twcMap)
                }
                "logout"->{
                    println("Log out")
                    logout(client,twcMap)
                }
                "getWorkspaces"->{
//                    println("Query Workspaces")
                    getWorkspaces(client,twcMap)
                }
                "getResources"->{
//                    println("Query Resources")
//                    println(obj)
                    getResources(client,twcMap,obj)
                    //(client,twcMap)
                }
                "getBranches"->{
//                    println("Query Branches")
                    getBranches(client,twcMap,obj)
                }
                "getRevisions"->{
//                    println("Query Revisions")
                    getRevisions(client,twcMap,obj)
                }
                "getRootElementIds"->{
//                    println("Search Root Element Ids")
                    getRootElementIds(client,twcMap,obj)
                }
                "getRootElement"->{
//                    println("Query Root Element")
                    getRootElement(client,twcMap,obj)
                }
                "getElement"->{
                    //println("Query Element")
                    getElement(client,twcMap,obj)
                }
                else -> error("Unknown Command: ${json.getString("event")}")
            }

        })


    }

    private fun  getElement(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        getRootElement(client,twcMap,obj)
    }

    private fun  getRootElement(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        vertx.sharedData().getCounter("queries", { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.incrementAndGet{}
            } else {
                error("Counter: queries not available.")
            }
        })
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val elementId = obj.getString(DataConstants.ELEMENT_ID)
        // TODO rewrite to Post
        client.get(port,serverPath,
                "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/elements/${elementId}")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .sendJson(JsonObject(), { ar ->
                    if(ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()

                            saveElement(elementId,data)

                            val element = Element(elementId,branchId,resourceId,workspaceId,
                                    data.getJsonObject(0).getJsonArray("ldp:contains"))

                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("element", element)))

                        } else {
                            println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                            myError()
                        }
                    } else {
                        println("Query Root Element failed: ${ar.cause().message}")
                        myError()
                    }

                })
    }

    private fun  saveElement(elementId: String, data: JsonArray) {
        vertx.sharedData().getCounter("sum", { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.incrementAndGet{}
            } else {
                error("Counter: queries not available.")
            }
        })
        vertx.sharedData().getCounter("number", { res ->
            if (res.succeeded()) {
                val counter = res.result()
                counter.incrementAndGet{}
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

    private fun  getRootElementIds(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        val revId = obj.getInteger("rev_id")
        client.get(port,serverPath,
                "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions/${revId}")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .send { ar ->
                    if(ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()
                            //println(data)
                            val revision = Revision(revId,branchId,resourceId,workspaceId,
                                    data.getJsonObject(0).getJsonArray("rootObjectIDs"))
                            //println(revision)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("revision", revision)))

                        } else {
                            println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                            myError()
                        }
                    } else {
                        println("Query Root Element IDs failed: ${ar.cause().message}")
                        myError()
                    }

                }
    }

    private fun  getRevisions(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        val branchId = obj.getString(DataConstants.BRANCH_ID)
        client.get(port,serverPath,
                "/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches/${branchId}/revisions")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .send { ar ->
                    if(ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()
                            val branch = Branch(branchId,resourceId,workspaceId,data.getJsonObject(0).getJsonArray("ldp:contains"))
                            //println(resource)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("branch", branch)))

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

    private fun  getBranches(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        val resourceId = obj.getString(DataConstants.RESOURCE_ID)
        client.get(port,serverPath,"/osmc/workspaces/${workspaceId}/resources/${resourceId}/branches")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .send { ar ->
                    if(ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonObject()
                            val resource = Resource(resourceId,workspaceId,data.getJsonArray("ldp:contains"))
                            //println(resource)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("resource", resource)))

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

    private fun  getResources(client: WebClient, twcMap: LocalMap<Any, Any>, obj: JsonObject) {
        val workspaceId = obj.getString(DataConstants.WORKSPACE_ID)
        client.get(port,serverPath,"/osmc/workspaces/${workspaceId}/resources")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .send { ar ->
                    if(ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonArray()
                            val workspace = Workspace(workspaceId,data.getJsonObject(0).getJsonArray("ldp:contains"))
                            //println(workspace)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("workspace", workspace)))

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

    private fun login(client: WebClient,twcMap : LocalMap<Any,Any>){
        client.get(port,serverPath,"/osmc/login")
                .putHeader("content-type","application/json")
                .putHeader("Accept","text/html")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .send({ ar ->
                    if(ar.succeeded()){
                        if(ar.result().statusCode()==204){
                            val userCookie = ar.result().cookies()[0].split(';')[0]
                            val sessionCookie = ar.result().cookies()[1].split(';')[0]
                            twcMap.put("user_cookie",userCookie)
                            twcMap.put("session_cookie",sessionCookie)
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("logged_in", JsonObject())))
                        }else{
                            println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                            myError()
                        }

                    }else{
                        ar.cause().printStackTrace()
                        println("Login failed: ${ar.cause().message}")
                        myError()
                    }
                })
    }

    private fun logout(client: WebClient,twcMap : LocalMap<Any,Any>){
        client.get(port,serverPath,"/osmc/logout")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .send { ar ->
                    if(ar.succeeded()){
                        if(ar.result().statusCode()==204){
                            //twcMap.put("cookies",ar.result().cookies().toString())
                            twcMap.remove("cookies")
                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS, Json.encode(Message("exit", JsonObject())))
                        }else{
                            println("${ar.result().statusCode()} : ${ar.result().statusMessage()}")
                            myError()
                        }

                    }else{
                        ar.cause().printStackTrace()
                        println("Logout failed: ${ar.cause().message}")
                        myError()
                    }
                }
    }

    //TODO
    private fun getWorkspaces(client: WebClient,twcMap : LocalMap<Any,Any>){
        client.get(port,serverPath,"/osmc/workspaces")
                .putHeader("content-type","application/ld+json")
                .putHeader("Authorization", "${twcMap.get("credential")}")
                .putHeader("Cookie","${twcMap.get("user_cookie")}")
                .putHeader("Cookie","${twcMap.get("session_cookie")}")
                .timeout(2000)
                .send { ar ->
                    if(ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {

                            val data = ar.result().bodyAsJsonObject()
                            val repoID = data.getString("@id")
                            val workspaces = data.getJsonArray("ldp:contains")

                            val repo = Repo(repoID,workspaces)

                            //println(repo)
                            twcMap.put("repo",Json.encode(repo))

                            vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS,Json.encode(Message("repo", JsonObject())))
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

    private fun myError(){
        vertx.eventBus().send(DataConstants.TWCMAIN_ADDRESS,Json.encode(Message("error", JsonObject())))
    }

}