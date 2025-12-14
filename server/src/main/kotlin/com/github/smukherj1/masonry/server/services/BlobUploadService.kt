package com.github.smukherj1.masonry.server.services

import com.github.smukherj1.masonry.server.extensions.toHexString
import com.github.smukherj1.masonry.server.models.BlobModel
import com.github.smukherj1.masonry.server.models.BlobUploadModel
import com.github.smukherj1.masonry.server.models.DigestModel
import com.github.smukherj1.masonry.server.models.HasherState
import com.github.smukherj1.masonry.server.models.UploadStatus
import com.github.smukherj1.masonry.server.repositories.BlobRepository
import com.github.smukherj1.masonry.server.repositories.BlobUploadRepository
import io.grpc.Status
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.IOException
import java.io.RandomAccessFile
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.jvm.optionals.getOrNull
import org.slf4j.Logger
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import kotlin.io.path.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.exists

@Service
class BlobUploadService(
    val blobUploadRepository: BlobUploadRepository,
    val blobRepository: BlobRepository
) {
    val log: Logger = LoggerFactory.getLogger(BlobUploadService::class.java)

    init {
        val up = Path(uploadsDir)
        log.info("Creating uploads directory: $up")
        up.createDirectories()
        val bp = Path(blobsDir)
        log.info("Creating blobs directory: $bp")
        bp.createDirectories()
    }

    fun upload(uploadId: String, serverUploadId: String, offset: Long, data: ByteArray): BlobUploadModel {
        val uploadModel = getOrCreateUpload(uploadId = uploadId, serverUploadId = serverUploadId)
        require(uploadModel.uploadStatus == UploadStatus.ACTIVE, {"unable to upload with id $uploadId because its state is currently ${uploadModel.uploadStatus}"})
        require(offset == uploadModel.nextOffset, {"given offset $offset does not match currently expected next offset ${uploadModel.nextOffset}"})
        if(uploadModel.nextOffset > 0 && uploadModel.hasherState.state == null) {
            blobUploadRepository.save(
                uploadModel.copy(
                    updateTime = localDateTimeNow(),
                    uploadStatus = UploadStatus.FAILED,
                )
            )
            throw RuntimeException("data hasher was not initialized despite ${uploadModel.nextOffset} bytes being uploaded, internal state is corrupted and upload is being marked as failed")
        }

        val digester = uploadModel.digester

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
                digester.update(data, 0, data.size)
                file.seek(uploadModel.nextOffset)
                file.write(data)
            }
        } catch (e: IOException) {
            log.error("Error uploading ${data.size} bytes of data for upload $uploadId, server ID $serverUploadId, offset $offset", e)
            throw RuntimeException("error writing ${data.size} bytes of data for upload $uploadId, offset $offset")
        }
        val now = localDateTimeNow()
        return blobUploadRepository.save(uploadModel.copy(
            updateTime = now,
            nextOffset = offset + data.size,
            hasherState = HasherState(digester.encodedState)
        ))
    }

    fun getUpload(uploadId: String): BlobUploadModel?  = blobUploadRepository.findById(uploadId).getOrNull()

    fun completeUpload(uploadId: String): BlobModel {
        val uploadModel = getUploadForCompletion(uploadId)
        val digester = uploadModel.digester
        val hash = ByteArray(digester.digestSize)
        digester.doFinal(hash, 0)
        val digestModel = DigestModel(hash=hash, sizeBytes = uploadModel.nextOffset)

        val sourcePath = Paths.get(uploadPath(uploadId))
        val destPathStr = blobPath(digestModel)
        val destPath = Paths.get(destPathStr)
        if(!destPath.exists()) {
            Files.move(sourcePath, destPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING)

        }

        val blob = blobRepository.save(
            BlobModel(
                digest = digestModel,
                createdAt = localDateTimeNow(),
                location = destPathStr

            )
        )
        blobUploadRepository.save(
            uploadModel.copy(
                uploadStatus = UploadStatus.COMPLETED,
                updateTime = localDateTimeNow(),
            )
        )
        return blob
    }

    @Transactional
    private fun getUploadForCompletion(uploadId: String): BlobUploadModel {
        val uploadModel = blobUploadRepository.findById(uploadId).getOrNull() ?: throw Status.NOT_FOUND.withDescription(
            "upload ID $uploadId does not exist"
        ).asRuntimeException()
        check(uploadModel.uploadStatus == UploadStatus.ACTIVE || uploadModel.uploadStatus == UploadStatus.FINALIZING, {
            "can't complete upload with status ${uploadModel.uploadStatus}, status must be ACTIVE or FINALIZING"
        })
        blobUploadRepository.save(uploadModel.copy(
            uploadStatus = UploadStatus.FINALIZING,
            updateTime = localDateTimeNow(),
        ))
        return uploadModel
    }

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
                hasherState = HasherState(),
                uploadStatus = UploadStatus.ACTIVE,
            ))
        }
        if (cur.serverUploadId != serverUploadId) {
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

private val uploadsDir = "uploads"
private val blobsDir = "blobs"

private fun uploadPath(uploadId: String) = "$uploadsDir/${uploadId}"

private fun blobPath(digest: DigestModel) = "$blobsDir/${digest.hash.toHexString()}-${digest.sizeBytes}"

private fun localDateTimeNow() = LocalDateTime.now(ZoneOffset.UTC)