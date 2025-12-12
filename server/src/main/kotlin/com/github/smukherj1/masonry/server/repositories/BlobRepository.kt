package com.github.smukherj1.masonry.server.repositories

import com.github.smukherj1.masonry.server.models.BlobModel
import com.github.smukherj1.masonry.server.models.DigestModel
import org.springframework.data.jpa.repository.JpaRepository

interface BlobRepository: JpaRepository<BlobModel, DigestModel> {
}