package com.github.smukherj1.masonry.server.models

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import java.time.LocalDateTime

@Entity
data class BlobUploadModel(
    @Id
    val uploadId: String,
    @Column(nullable = false)
    val createTime: LocalDateTime,
    @Column(nullable = false)
    val updateTime: LocalDateTime,
    @Column(nullable = false)
    val location: String,
    @Column(nullable = false)
    val nextOffset: ULong,
)
