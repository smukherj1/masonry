package com.github.smukherj1.masonry.server.models

import jakarta.persistence.Column
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import java.time.LocalDateTime

@Entity
data class BlobModel(
    @EmbeddedId
    val digest: DigestModel,
    @Column(nullable = false)
    val createdAt: LocalDateTime,
    @Column(nullable = false)
    val location: String,
)