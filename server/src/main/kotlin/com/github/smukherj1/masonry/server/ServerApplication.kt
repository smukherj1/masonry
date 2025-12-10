package com.github.smukherj1.masonry.server

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ServerApplication

fun main(args: Array<String>) {
	runApplication<ServerApplication>(*args)
}
