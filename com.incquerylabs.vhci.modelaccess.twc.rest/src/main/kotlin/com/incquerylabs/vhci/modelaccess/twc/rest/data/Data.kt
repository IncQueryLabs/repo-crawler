package com.incquerylabs.vhci.modelaccess.twc.rest.data

import io.vertx.core.json.JsonArray

data class Message(val event:String, val obj:Any)
data class User(val username:String, val password:String)
data class Server(val path:String, val port:Int)
data class Repo(val id: String, val workspaces:JsonArray)
data class Workspace(val id:String, val resources:JsonArray)
data class Resource(val id:String, val workspace_id:String, val branches:JsonArray)
data class Branch(val id:String,val resource_id:String ,val workspace_id:String,val revisions:JsonArray)
data class Revision(val id:Int, val branch_id:String, val resource_id:String ,val workspace_id:String, val elements:JsonArray)
data class Element(val id:String, val branch_id:String, val resource_id:String ,val workspace_id:String, val elements:JsonArray)