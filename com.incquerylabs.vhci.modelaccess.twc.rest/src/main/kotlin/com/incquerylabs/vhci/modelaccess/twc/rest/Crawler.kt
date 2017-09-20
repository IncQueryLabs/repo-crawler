package com.incquerylabs.vhci.modelaccess.twc.rest

import com.incquerylabs.vhci.modelaccess.twc.rest.data.*
import io.vertx.core.Vertx
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import com.incquerylabs.vhci.modelaccess.twc.rest.verticles.MainVerticle
import com.incquerylabs.vhci.modelaccess.twc.rest.verticles.RESTVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.cli.Argument
import io.vertx.core.cli.CLI
import io.vertx.core.cli.Option
import io.vertx.core.shareddata.LocalMap
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import java.io.File



fun main(args: Array<String>) {

    val cli = CLI.create("crawler")
            .setSummary("A REST Client to query all model element from server.")
            .addArgument(Argument()
                    .setArgName("username")
                    .setIndex(0)
                    .setDefaultValue("admin")
                    .setDescription("TWC username."))
            .addArgument(Argument()
                    .setArgName("password")
                    .setIndex(1)
                    .setDefaultValue("admin")
                    .setDescription("TWC password."))
            .addOption(Option()
                    .setLongName("help")
                    .setShortName("h")
                    .setDescription("Show help site.")
                    .setFlag(true))
            .addOption(Option()
                    .setLongName("server")
                    .setShortName("S")
                    .setDescription("Set server path."))
            .addOption(Option()
                    .setLongName("port")
                    .setShortName("P")
                    .setDescription("Set server port number."))
            .addOption(Option()
                    .setLongName("instanceNum")
                    .setShortName("I")
                    .setDescription("Set number of RESTVerticle instances. Default:16")
                    .setDefaultValue("16"))


    val commandLine = cli.parse(args.asList(),false)

    if(commandLine.isFlagEnabled("help")) {
        val builder = StringBuilder()
        cli.usage(builder)
        println(builder.toString())
        return
    }

    val vertx = Vertx.vertx()
    val sd = vertx.sharedData()
    val twcMap = sd.getLocalMap<Any,Any>("twcMap")

    val serverOpt = commandLine.getOptionValue<String>("server")
    val portOpt = commandLine.getOptionValue<String>("port")
    val instanceNum = commandLine.getOptionValue<String>("instanceNum").toInt()

    if(instanceNum!=null){
        println(instanceNum)
        if(instanceNum<1){
            error("Number of Instances should be at least 1.")
        }
    }

    if(serverOpt != null && portOpt!=null){
        if(!File("server.config").exists()){
            File("server.config").createNewFile()
        }

        File("server.config").writeText(Json.encode(Server(serverOpt,portOpt.toInt())))
        twcMap.put("server_path",serverOpt)
        twcMap.put("server_port",portOpt.toInt())
        println("New Server config")

    } else {
        if(File("server.config").exists()) {
            val config = File("server.config").readText()

            if (config != "") {
                val serverConf = JsonObject(config)
                val serverPath = serverConf.getString("path")
                val serverPort = serverConf.getInteger("port")
                twcMap.put("server_path", serverPath)
                twcMap.put("server_port", serverPort)
            } else {
                error("Server config is empty.\n" +
                        "Please set Server Path and Port.\n" +
                        "(Check usage with -h (--help) cli option)")
            }
        } else {
            error("No server config found.\n" +
                    "Please set Server Path and Port.\n" +
                    "(Check usage with -h (--help) cli option)")
        }
    }
    println("Server config: ${twcMap.get("server_path")}:${twcMap.get("server_port")}")

    val restVerticle = RESTVerticle()

    var options = DeploymentOptions().setWorker(true).setHa(true).setInstances(instanceNum).setWorkerPoolSize(32)

    twcMap.put("flag",0)
    vertx.deployVerticle(restVerticle.javaClass.name,options,{ deploy ->
        if (deploy.failed()) {
            error("Deploy failed: ${deploy.cause().message}")
        } else {

            val usr = commandLine.getArgumentValue<String>("username")
            val pswd = commandLine.getArgumentValue<String>("password")
            twcMap.put("credential","Basic ${java.util.Base64.getEncoder().encodeToString("${usr}:${pswd}".toByteArray())}")
            twcMap.put("username",usr)

            vertx.deployVerticle(MainVerticle(usr,pswd),{deploy->
                if(deploy.failed()){
                    error("Deploy failed: ${deploy.cause().message}\n${deploy.cause().printStackTrace()}")
                }
            })
        }
    })

}

