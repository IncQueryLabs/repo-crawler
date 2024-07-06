package com.incquerylabs.twc.repo.crawler.data

import io.vertx.ext.web.client.WebClientOptions

data class CrawlerConfiguration(
    val debug: Boolean,
    val server: Server,
    val user: User,
    val webClientOptions: WebClientOptions,
    val chunkSize: Int,
    val requestTimeout: Long = 180
)

data class MainConfiguration(
    val user: User,
    val requestSingleElement: Boolean,
    val changeLogInterval: Int = 10,
    val infoLogInterval: Int = 60
)

data class Message(val event: String = "", val obj: Any = "")
data class User(val username: String, val password: String)
data class Server(val path: String, val port: Int, val ssl: Boolean)
data class Repo(val id: String = "", val workspaces: List<Any?> = emptyList())
data class Workspace(val id: String = "", val title: String = "", val resources: List<Any?> = emptyList())
data class Resource(val id: String = "", val title: String = "", val workspace_id: String = "", val workspace_title: String="", val branches: List<Any?> = emptyList())
data class Branch(
        val id: String = "",
        val title: String = "",
        val resource_id: String = "",
        val resource_title: String = "",
        val workspace_id: String = "",
        val workspace_title: String = "",
        val revisions: List<Any?> = emptyList()
)

data class Revision(
    val id: Int = -1,
    val branch_id: String = "",
    val resource_id: String = "",
    val workspace_id: String = "",
    val elements: List<String> = emptyList()
)

data class Element(
    val id: String = "",
    val revision_id: Int = -1,
    val branch_id: String = "",
    val resource_id: String = "",
    val workspace_id: String = "",
    val elements: List<Any?> = emptyList()
)

data class Elements(
    val revision_id: Int = -1,
    val branch_id: String = "",
    val resource_id: String = "",
    val workspace_id: String = "",
    val elements: Map<String, String> = mapOf()
)

const val REPO = "repo"
const val WORKSPACE = "workspace"
const val WORKSPACE_ID = "workspace_id"
const val WORKSPACE_TITLE = "workspace_title"
const val RESOURCE = "resource"
const val RESOURCE_ID = "resource_id"
const val RESOURCE_TITLE = "resource_title"
const val BRANCH = "branch"
const val BRANCH_ID = "branch_id"
const val BRANCH_TITLE = "branch_title"
const val REVISION = "revision"
const val REVISION_ID = "revision_id"
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

const val USER = "user_cookie"
const val SESSION = "session_cookie"

const val CHUNK_SIZE = "chunkSize"
const val REQUEST_TIMEOUT = "timeout"
const val MAX_HTTP_POOL_SIZE = "maxHttpPoolSize"
const val TRUST_ALL_SSL_CERT = "trustAll"
