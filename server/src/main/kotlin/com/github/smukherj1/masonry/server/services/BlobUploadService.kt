package com.github.smukherj1.masonry.server.services

import com.github.smukherj1.masonry.server.models.BlobUploadModel
import com.github.smukherj1.masonry.server.models.UploadStatus
import com.github.smukherj1.masonry.server.repositories.BlobRepository
import com.github.smukherj1.masonry.server.repositories.BlobUploadRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.IOException
import java.io.RandomAccessFile
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.jvm.optionals.getOrNull

@Service
class BlobUploadService(
    val blobUploadRepository: BlobUploadRepository,
    val blobRepository: BlobRepository
) {
    val log = LoggerFactory.getLogger(BlobUploadService::class.java)
    fun upload(uploadId: String, serverUploadId: String, offset: Long, data: ByteArray) {
        val uploadModel = getOrCreateUpload(uploadId = uploadId, serverUploadId = serverUploadId)
        require(uploadModel.uploadStatus == UploadStatus.ACTIVE, {"unable to upload with id $uploadId because its state is currently ${uploadModel.uploadStatus}"})
        require(offset == uploadModel.nextOffset, {"given offset $offset does not match currently expected next offset ${uploadModel.nextOffset}"})

        try {
            RandomAccessFile(uploadModel.location, "rw").use { file ->
                if (file.length() < uploadModel.nextOffset) {
                    // The upload model should only be updated after we've successfully committed data to the file.
                    // Thus, the file length being lower than the next expected offset means bytes disappeared or
                    // some other kind of corruption. Abandon the upload.
                    blobUploadRepository.save(
                        uploadModel.copy(
                            updateTime = localDateTimeNow(),
                            uploadStatus = UploadStatus.FAILED
                        )
                    )
                    throw RuntimeException("internal committed data offset for upload $uploadId did not match the expected offset ${uploadModel.nextOffset}, upload $uploadId is possibly corrupted and is being marked as failed")
                }
                file.seek(uploadModel.nextOffset)
                file.write(data)
            }
        } catch (e: IOException) {
            log.error("Error uploading ${data.size} bytes of data for upload $uploadId, server ID $serverUploadId, offset $offset", e)
            throw RuntimeException("error writing ${data.size} bytes of data for upload $uploadId, offset $offset")
        }
        val now = localDateTimeNow()
        blobUploadRepository.save(uploadModel.copy(
            updateTime = now,
            nextOffset = offset + data.size,
        ))
    }

    fun getUpload(uploadId: String): BlobUploadModel?  = blobUploadRepository.findById(uploadId).getOrNull()

    @Transactional
    private fun getOrCreateUpload(uploadId: String, serverUploadId: String): BlobUploadModel {
        require(uploadId.isNotBlank()) { "upload id must not be blank" }
        require(serverUploadId.isNotBlank()) { "server upload id must not be blank" }
        val cur = blobUploadRepository.findById(uploadId).getOrNull()
        val now = localDateTimeNow()

        if(cur == null) {
            return blobUploadRepository.save(BlobUploadModel(
                uploadId = uploadId,
                serverUploadId = serverUploadId,
                createTime = now,
                updateTime = now,
                nextOffset = 0L,
                location = uploadPath(uploadId),
                uploadStatus = UploadStatus.ACTIVE,
            ))
        }
        if (!cur.serverUploadId.equals(serverUploadId)) {
            blobUploadRepository.save(cur.copy(
                updateTime = now,
                uploadStatus = UploadStatus.FAILED
            ))
            throw IllegalStateException("Possible concurrent uploads detected for upload ID $uploadId, upload is now considered corrupted and a new upload with a different ID must be attempted")
        }
        return blobUploadRepository.save(cur.copy(
            updateTime = now,
        ))
    }
}

private fun uploadPath(uploadId: String) = "uploads/${uploadId}"

private fun localDateTimeNow() = LocalDateTime.now(ZoneOffset.UTC)