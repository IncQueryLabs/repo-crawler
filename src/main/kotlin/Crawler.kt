package com.incquerylabs.twc.repo.crawler

import com.incquerylabs.twc.repo.crawler.data.BRANCH_ID
import com.incquerylabs.twc.repo.crawler.data.CHUNK_SIZE
import com.incquerylabs.twc.repo.crawler.data.CrawlerConfiguration
import com.incquerylabs.twc.repo.crawler.data.MAX_HTTP_POOL_SIZE
import com.incquerylabs.twc.repo.crawler.data.MainConfiguration
import com.incquerylabs.twc.repo.crawler.data.RESOURCE_ID
import com.incquerylabs.twc.repo.crawler.data.REVISION
import com.incquerylabs.twc.repo.crawler.data.Server
import com.incquerylabs.twc.repo.crawler.data.User
import com.incquerylabs.twc.repo.crawler.data.WORKSPACE_ID
import com.incquerylabs.twc.repo.crawler.verticles.MainVerticle
import com.incquerylabs.twc.repo.crawler.verticles.RESTVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.cli.CLI
import io.vertx.core.cli.CommandLine
import io.vertx.core.cli.Option
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClientOptions
import java.io.File
import java.util.*

fun main(args: Array<String>) {

    val cli = defineCommandLineInterface()

    val commandLine = cli.parse(args.asList(), false)

    try {
        executeCrawler(commandLine, cli)
    } catch (e: Exception) {
        println("Exception occurred: $e")
        val builder = StringBuilder()
        cli.usage(builder)
        println(builder.toString())
    }

}

private fun executeCrawler(commandLine: CommandLine, cli: CLI) {
    if (commandLine.isFlagEnabled("help")) {
        val builder = StringBuilder()
        cli.usage(builder)
        println(builder.toString())
        return
    }

    val vertx = Vertx.vertx()
    val sd = vertx.sharedData()
    val twcMap = sd.getLocalMap<Any, Any>("twcMap")

    val usr = commandLine.getOptionValue<String>("username")
    val pswd = commandLine.getOptionValue<String>("password")
    val serverOpt = commandLine.getOptionValue<String>("server")
    val portOpt = commandLine.getOptionValue<String>("port")
    val isSslEnabled = commandLine.isFlagEnabled("ssl")
    val instanceNum = commandLine.getOptionValue<String>("instanceNum").toInt()
    val workspaceId = commandLine.getOptionValue<String>("workspaceId")
    val resourceId = commandLine.getOptionValue<String>("resourceId")
    val branchId = commandLine.getOptionValue<String>("branchId")
    val revision = commandLine.getOptionValue<String>("revision")
    val chunkSize = commandLine.getOptionValue<String>(CHUNK_SIZE).toInt()
    val maxPoolSize = commandLine.getOptionValue<String>(MAX_HTTP_POOL_SIZE).toInt()
    val debug = commandLine.isFlagEnabled("debug")
    val requestSingleElement = commandLine.isFlagEnabled("requestSingleElement")

    twcMap["debug"] = debug
    if (debug) {
        println("Debug mode is enabled")
    }

    twcMap["requestSingleElement"] = requestSingleElement
    if (requestSingleElement) {
        println("Request single elements mode is enabled")
    }

    twcMap[CHUNK_SIZE] = chunkSize
    println("Chunk size is $chunkSize")

    println("Instance number set to $instanceNum")
    if (instanceNum < 1) {
        error("Number of Instances should be at least 1.")
    }
    if (workspaceId != null) {
        println("Workspace ID set to $workspaceId")
        twcMap[WORKSPACE_ID] = workspaceId
    }
    if (resourceId != null) {
        println("Resource ID set to $resourceId")
        twcMap[RESOURCE_ID] = resourceId
    }
    if (branchId != null) {
        println("Branch ID set to $branchId")
        twcMap[BRANCH_ID] = branchId
    }
    if (revision != null) {
        println("Revision set to $revision")
        twcMap[REVISION] = revision.toInt()
    }


    val server = if (serverOpt != null && portOpt != null) {
        if (!File("server.config").exists()) {
            File("server.config").createNewFile()
        }

        val server = Server(
            serverOpt,
            portOpt.toInt(),
            isSslEnabled
        )
        File("server.config").writeText(
            Json.encode(
                server
            )
        )
        twcMap["server_path"] = serverOpt
        twcMap["server_port"] = portOpt.toInt()
        twcMap["server_ssl"] = isSslEnabled
        println("New Server config")
        server

    } else {
        if (File("server.config").exists()) {
            val config = File("server.config").readText()

            if (config != "") {
                val serverConf = JsonObject(config)
                val serverPath = serverConf.getString("path")
                val serverPort = serverConf.getInteger("port")
                val sslEnabled = serverConf.getBoolean("ssl")
                twcMap["server_path"] = serverPath
                twcMap["server_port"] = serverPort
                twcMap["server_ssl"] = sslEnabled
                Server(serverPath, serverPort, sslEnabled)
            } else {
                error(
                    "Server config is empty.\n" +
                            "Please set Server Path and Port.\n" +
                            "(Check usage with -h (--help) cli option)"
                )
            }
        } else {
            error(
                "No server config found.\n" +
                        "Please set Server Path and Port.\n" +
                        "(Check usage with -h (--help) cli option)"
            )
        }
    }
    println("Server config: ${twcMap["server_path"]}:${twcMap["server_port"]} (ssl: ${twcMap["server_ssl"]})")

    val configuration = CrawlerConfiguration(
        debug,
        server,
        User(usr, pswd),
        WebClientOptions().setSsl(isSslEnabled).setMaxPoolSize(maxPoolSize),
        chunkSize
    )

    val restVerticle = RESTVerticle(configuration)

    val options = DeploymentOptions().setWorker(true)

    twcMap["flag"] = 0
    vertx.deployVerticle(restVerticle, options) { deploy ->
        if (deploy.failed()) {
            error("Deploy failed: ${deploy.cause().message}")
        } else {

            twcMap["credential"] =
                "Basic ${Base64.getEncoder().encodeToString("${usr}:${pswd}".toByteArray())}"
            twcMap["username"] = usr

            vertx.deployVerticle(
                MainVerticle(MainConfiguration(User(usr, pswd), requestSingleElement))
            ) {
                if (it.failed()) {
                    error("Deploy failed: ${it.cause().message}\n${it.cause().printStackTrace()}")
                }
            }
        }
    }
}

private fun defineCommandLineInterface(): CLI {
    return CLI.create("crawler")
        .setSummary("A REST Client to query all model element from server.")
        .addOptions(
            listOf(
                createOption("help", "h", "Show usage description")
                    .setFlag(true),
                createOption("username", "u", "TWC username")
                    .setDefaultValue("admin"),
                createOption("password", "pw", "TWC password")
                    .setDefaultValue("admin"),
                createOption("server", "S", "Set server path"),
                createOption("port", "P", "Set server port number"),
                createOption("ssl", "ssl", "SSL server connection. Default: false")
                    .setFlag(true),
                createOption("instanceNum", "I", "Set number of RESTVerticle instances. Default: 4")
                    .setDefaultValue("4"),
                createOption("workspaceId", "W", "Select workspace to crawl"),
                createOption("resourceId", "R", "Select resource to crawl"),
                createOption("branchId", "B", "Select branch to crawl"),
                createOption("revision", "REV", "Select revision to crawl"),
                createOption("debug", "D", "Enable debug logging. Default: false")
                    .setFlag(true),
                createOption("requestSingleElement", "RSE", "Request elements one-by-one. Default: false")
                    .setFlag(true),
                createOption(
                    CHUNK_SIZE,
                    "C",
                    "Set the size of chunks to use when crawling elements (-1 to disable chunks). Default: 2000"
                )
                    .setDefaultValue("2000"),
                createOption(MAX_HTTP_POOL_SIZE, "MPS", "Number of concurrent requests. Default: 1")
                    .setDefaultValue("1")
            )
        )
}

private fun createOption(longName: String, shortName: String, description: String): Option {
    return Option()
        .setLongName(longName)
        .setShortName(shortName)
        .setDescription(description)
}

