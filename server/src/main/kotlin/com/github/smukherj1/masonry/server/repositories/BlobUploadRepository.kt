package com.github.smukherj1.masonry.server.repositories

import com.github.smukherj1.masonry.server.models.BlobUploadModel
import org.springframework.data.jpa.repository.JpaRepository

interface BlobUploadRepository: JpaRepository<BlobUploadModel, String> {
}