package com.github.smukherj1.masonry.server.extensions

fun ByteArray.toHexString(): String {
    return joinToString("") { "%02x".format(it) }
}