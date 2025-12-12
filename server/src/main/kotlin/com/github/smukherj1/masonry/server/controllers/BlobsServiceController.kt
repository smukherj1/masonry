package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.models.BlobUploadModel
import com.github.smukherj1.masonry.server.proto.BeginUploadRequest
import com.github.smukherj1.masonry.server.proto.BeginUploadResponse
import com.github.smukherj1.masonry.server.proto.BlobsServiceGrpc
import com.github.smukherj1.masonry.server.repositories.BlobRepository
import com.github.smukherj1.masonry.server.repositories.BlobUploadRepository
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@GrpcService
class BlobsServiceController(
    val blobUploadRepository: BlobUploadRepository,
    var blobRepository: BlobRepository
    ) : BlobsServiceGrpc.BlobsServiceImplBase() {

    val log = LoggerFactory.getLogger(BlobsServiceController::class.java)

    override fun beginUpload(request: BeginUploadRequest?, responseObserver: StreamObserver<BeginUploadResponse?>?) {
        log.info("beginUpload(): uploadId=${request?.uploadId}")
        if(responseObserver == null) return
        if(request?.uploadId.isNullOrBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("uploadId cannot be null or blank").asRuntimeException())
            return
        }
        if(!createUpload(request.uploadId)) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("uploadId ${request.uploadId} has already been claimed").asRuntimeException())
        }
        responseObserver.onNext(BeginUploadResponse.newBuilder().setUploadId(request.uploadId).build())
        responseObserver.onCompleted()
    }

    @Transactional
    private fun createUpload(uploadId: String): Boolean {
        if(blobUploadRepository.existsById(uploadId)) {
            return false
        }
        val now = LocalDateTime.now()
        blobUploadRepository.save(BlobUploadModel(
            uploadId = uploadId,
            createTime = now,
            updateTime = now,
            location = "uploads/$uploadId",
            nextOffset = 0UL))
        return true
    }
}