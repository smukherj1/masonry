package com.github.smukherj1.masonry.server.models

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import java.io.Serializable

@Embeddable
data class DigestModel(
    @Column(nullable = false)
    val hash: ByteArray,
    @Column(nullable = false)
    val sizeBytes: Long,
) : Serializable {
    override fun equals(other: Any?): Boolean {
        if(this === other) return true
        if(javaClass != other?.javaClass) return false
        other as DigestModel
        if(sizeBytes != other.sizeBytes) return false
        if(!hash.contentEquals(other.hash)) return false
        return true
    }

    override fun hashCode(): Int {
        return sizeBytes.hashCode() * 31 + hash.contentHashCode()
    }

    override fun toString(): String {
        return "${hash.toHexString()}/${sizeBytes.toHexString()}"
    }
}
