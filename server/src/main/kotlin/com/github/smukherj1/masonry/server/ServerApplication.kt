package com.github.smukherj1.masonry.server

import com.github.smukherj1.masonry.server.configuration.ServerConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class ServerApplication {
	@Autowired
	private lateinit var serverConfiguration: ServerConfiguration
}

fun main(args: Array<String>) {
	runApplication<ServerApplication>(*args)
}
