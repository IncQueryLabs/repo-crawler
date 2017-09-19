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
    const val REPO = "repo"
    const val WORKSPACE = "workspace"
    const val WORKSPACE_ID = "workspace_id"
    const val RESOURCE = "resource"
    const val RESOURCE_ID = "resource_id"
    const val BRANCH = "branch"
    const val BRANCH_ID = "branch_id"
    const val REVISION = "revision"
    const val REVISION_ID = "rev_id"
    const val ELEMENTS = "elements"
    const val ELEMENT_IDS = "element_ids"
    const val ELEMENT = "element"
    const val ELEMENT_ID = "element_id"

    const val GET_WORKSPACES = "getWorkspaces"
    const val GET_RESOURCES = "getResources"
    const val GET_BRANCHES = "getBranches"
    const val GET_REVISIONS = "getRevisions"
    const val GET_ROOT_ELEMENT_IDS = "getRootElementIds"
    const val GET_ELEMENT = "getElement"
    const val GET_ELEMENTS = "getElements"

    const val TWCMAP = "twcMap"
    const val TWCVERT_ADDRESS = "twc.rest.twcvert"
    const val TWCMAIN_ADDRESS = "twc.rest.main"
    const val QUERIES = "queries"
    const val EXIT = "exit"
    const val ERROR = "error"
    const val LOGIN = "login"
    const val LOGGED_IN = "logged_in"
    const val LOGOUT = "logout"
}
