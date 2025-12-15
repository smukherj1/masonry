package com.github.smukherj1.masonry.server.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding

@ConfigurationProperties(prefix = "masonry")
data class ServerConfiguration @ConstructorBinding constructor(
    val uploadsDir: String,
    val blobsDir: String,
) {
    init {
        require(blobsDir.isNotBlank(), ) { "blobsDir cannot be blank" }
        require(uploadsDir.isNotBlank(), ){ "uploadsDir cannot be blank" }
    }
}
