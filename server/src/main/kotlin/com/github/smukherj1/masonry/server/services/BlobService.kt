package com.github.smukherj1.masonry.server.services

import com.github.smukherj1.masonry.server.models.DigestModel
import com.github.smukherj1.masonry.server.repositories.BlobRepository
import io.grpc.Status
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.IOException
import java.io.RandomAccessFile
import kotlin.jvm.optionals.getOrNull
import kotlin.math.min

@Service
class BlobService(
    val blobRepository: BlobRepository
) {
    val log: Logger = LoggerFactory.getLogger(BlobUploadService::class.java)

    fun query(digestModel: DigestModel) = blobRepository.findById(digestModel).getOrNull()
        ?: throw Status.NOT_FOUND.withDescription("blob with digest ${digestModel.toString()} not found")
            .asRuntimeException()

    fun download(digestModel: DigestModel, offset: Long, limit: Long, onNextChunk: (offset: Long, data: ByteArray) -> Unit) {
        val blobModel = query(digestModel)
        require(offset >= 0) { "offset must be positive" }
        require(limit <= Int.MAX_VALUE) { "limit can't exceed ${Int.MAX_VALUE}" }
        require(limit >= 0) { "limit must be positive" }
        require(offset <= blobModel.digest.sizeBytes, { "offset $offset exceeds size of blob ${blobModel.digest.sizeBytes}" })

        try {
            RandomAccessFile(blobModel.location, "r").use { file ->
                file.seek(offset)
                val chunkSize = if (limit > 0) { min(limit.toInt(), maxDownloadChunkSize) } else { maxDownloadChunkSize}
                val totalBytesToRead = if (limit > 0) { min(limit, file.length()) } else { file.length() }
                var totalBytesRead = 0L

                val buffer = ByteArray(chunkSize)
                while (true) {
                    val bytesRead = file.read(buffer)
                    if (bytesRead == -1) break

                    if((totalBytesRead + bytesRead) <= totalBytesToRead) {
                        onNextChunk(
                            totalBytesRead,
                            buffer
                        )
                    } else {
                        val bytesToCopy = (totalBytesToRead - totalBytesRead)
                        // Safety check for the toInt conversion below. Should never happen given we currently set
                        // max chunk size to 4 MB but this is mostly defensive coding for the future.
                        if (bytesToCopy > Int.MAX_VALUE) {
                            log.error(
                                "While streaming blob ${digestModel.toString()}, " +
                                        "got last chunk size $bytesToCopy which exceeds 32-bit Int Max, " +
                                        "request(offset=$offset, limit=$limit), " +
                                        "chunk size $chunkSize, " +
                                        "total bytes to read $totalBytesToRead, " +
                                        "total bytes read $totalBytesRead"
                            )
                            throw RuntimeException("internal blob chunk size exceeds 32-bit Int Max")
                        }

                        onNextChunk(
                            totalBytesRead,
                            buffer.copyOfRange(0, bytesToCopy.toInt())
                        )
                    }

                    totalBytesRead += bytesRead
                    if(totalBytesRead >= totalBytesToRead) break
                }
            }

        } catch(e: IOException) {
            log.error("Error while downloading ${digestModel.toString()}, offset=$offset, limit=$limit", e)
            throw RuntimeException("error streaming contents of blob ${digestModel.toString()}")
        }
    }

}

const val maxDownloadChunkSize: Int = 4 * 1024 * 1024 * 1024 - 1024