package com.incquerylabs.twc.repo.crawler

import com.incquerylabs.twc.repo.crawler.data.*
import com.incquerylabs.twc.repo.crawler.verticles.MainVerticle
import com.incquerylabs.twc.repo.crawler.verticles.RESTVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.cli.CLI
import io.vertx.core.cli.Option
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import java.io.File

const val CHUNK_SIZE = "chunkSize"

fun main(args: Array<String>) {

    val cli = CLI.create("crawler")
        .setSummary("A REST Client to query all model element from server.")
        .addOptions(
            listOf(
                Option()
                    .setLongName("help")
                    .setShortName("h")
                    .setDescription("Show help site.")
                    .setFlag(true),
                Option()
                    .setLongName("username")
                    .setShortName("u")
                    .setDefaultValue("admin")
                    .setDescription("TWC username."),
                Option()
                    .setLongName("password")
                    .setShortName("pw")
                    .setDefaultValue("admin")
                    .setDescription("TWC password."),
                Option()
                    .setLongName("server")
                    .setShortName("S")
                    .setDescription("Set server path."),
                Option()
                    .setLongName("port")
                    .setShortName("P")
                    .setDescription("Set server port number."),
                Option()
                    .setLongName("ssl")
                    .setShortName("ssl")
                    .setDescription("SSL server connection. Default: false")
                    .setFlag(true),
                Option()
                    .setLongName("instanceNum")
                    .setShortName("I")
                    .setDescription("Set number of RESTVerticle instances. Default: 4")
                    .setDefaultValue("4"),
                Option()
                    .setLongName("workspaceId")
                    .setShortName("W")
                    .setDescription("Select workspace to crawl"),
                Option()
                    .setLongName("resourceId")
                    .setShortName("R")
                    .setDescription("Select resource to crawl"),
                Option()
                    .setLongName("branchId")
                    .setShortName("B")
                    .setDescription("Select branch to crawl"),
                Option()
                    .setLongName("revision")
                    .setShortName("REV")
                    .setDescription("Select revision to crawl"),
                Option()
                    .setLongName("debug")
                    .setShortName("D")
                    .setDescription("Enable debug logging. Default: false")
                    .setFlag(true),
                Option()
                    .setLongName("requestSingleElement")
                    .setShortName("RSE")
                    .setDescription("Request elements one-by-one. Default: false")
                    .setFlag(true),
                Option()
                    .setLongName(CHUNK_SIZE)
                    .setShortName("C")
                    .setDescription("Set the size of chunks to use when crawling elements (-1 to disable chunks). Default: 2000")
                    .setDefaultValue("2000")
            )
        )


    val commandLine = cli.parse(args.asList(), false)

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


    if (serverOpt != null && portOpt != null) {
        if (!File("server.config").exists()) {
            File("server.config").createNewFile()
        }

        File("server.config").writeText(Json.encode(
            Server(
                serverOpt,
                portOpt.toInt(),
                isSslEnabled
            )
        ))
        twcMap["server_path"] = serverOpt
        twcMap["server_port"] = portOpt.toInt()
        twcMap["server_ssl"] = isSslEnabled
        println("New Server config")

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

    val restVerticle = RESTVerticle()

    val options = DeploymentOptions().setWorker(true).setHa(true).setInstances(instanceNum).setWorkerPoolSize(32)

    twcMap["flag"] = 0
    vertx.deployVerticle(restVerticle.javaClass.name, options) { deploy ->
        if (deploy.failed()) {
            error("Deploy failed: ${deploy.cause().message}")
        } else {

            twcMap["credential"] =
                "Basic ${java.util.Base64.getEncoder().encodeToString("${usr}:${pswd}".toByteArray())}"
            twcMap["username"] = usr

            vertx.deployVerticle(
                MainVerticle(
                    usr,
                    pswd
                )
            ) {
                if (it.failed()) {
                    error("Deploy failed: ${it.cause().message}\n${it.cause().printStackTrace()}")
                }
            }
        }
    }

}

