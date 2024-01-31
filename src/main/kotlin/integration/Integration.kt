package com.incquerylabs.twc.repo.crawler.integration

import io.vertx.core.json.JsonObject

interface ContentHandler {

    fun handleContent(content: JsonObject, serverHost: String, workspaceId: String, resourceId: String, branchId: String, revision: Int): Unit = Unit

}

class NoopContentHandler: ContentHandler
