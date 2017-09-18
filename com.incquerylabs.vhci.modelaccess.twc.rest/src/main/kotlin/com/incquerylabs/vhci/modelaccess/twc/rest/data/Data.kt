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

object DataConstants {
    const val WORKSPACE_ID = "workspace_id"
    const val RESOURCE_ID = "resource_id"
    const val BRANCH_ID = "branch_id"
    const val ELEMENT_ID = "element_id"
    const val TWCMAP = "twcMap"
    const val ELEMENTS = "elements"
    const val QUERIES = "queries"
    const val TWCVERT_ADDRESS = "twc.rest.twcvert"
    const val TWCMAIN_ADDRESS = "twc.rest.main"
    const val ERROR = "error"
    const val EXIT = "exit"
}
