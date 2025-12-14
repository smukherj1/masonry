package com.github.smukherj1.masonry.server.services

import com.github.smukherj1.masonry.server.repositories.BlobRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class BlobService(
    val blobRepository: BlobRepository
) {
    val log: Logger = LoggerFactory.getLogger(BlobUploadService::class.java)
}