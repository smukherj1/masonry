package com.github.smukherj1.masonry.server.services

import com.github.smukherj1.masonry.server.models.BlobUploadModel
import com.github.smukherj1.masonry.server.repositories.BlobRepository
import com.github.smukherj1.masonry.server.repositories.BlobUploadRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class BlobUploadService(
    val blobUploadRepository: BlobUploadRepository,
    val blobRepository: BlobRepository
) {

    @Transactional
    fun beginUpload(uploadId: String) {
        require(uploadId.isNotBlank()) { "upload id must not be blank" }
        check(!blobUploadRepository.existsById(uploadId)) { "Upload ID '${uploadId}' already exists" }
        val now = LocalDateTime.now(ZoneOffset.UTC)
        blobUploadRepository.save(BlobUploadModel(
            uploadId = uploadId,
            createTime = now,
            updateTime = now,
            nextOffset = 0L,
            location = uploadPath(uploadId)
        ))
    }
}

private fun uploadPath(uploadId: String) = "uploads/${uploadId}"