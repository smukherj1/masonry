package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.errors.safeRun
import com.github.smukherj1.masonry.server.models.BlobUploadModel
import com.github.smukherj1.masonry.server.proto.BeginUploadRequest
import com.github.smukherj1.masonry.server.proto.BeginUploadResponse
import com.github.smukherj1.masonry.server.proto.BlobsServiceGrpc
import com.github.smukherj1.masonry.server.repositories.BlobRepository
import com.github.smukherj1.masonry.server.repositories.BlobUploadRepository
import com.github.smukherj1.masonry.server.services.BlobUploadService
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@GrpcService
class BlobsServiceController(
    val blobUploadService: BlobUploadService
    ) : BlobsServiceGrpc.BlobsServiceImplBase() {

    val log = LoggerFactory.getLogger(BlobsServiceController::class.java)

    override fun beginUpload(request: BeginUploadRequest?, responseObserver: StreamObserver<BeginUploadResponse?>?) {
        log.info("beginUpload(): uploadId=${request?.uploadId}")
        if(responseObserver == null) return
        if(request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("request was null").asRuntimeException())
            return
        }

        responseObserver.safeRun {
            blobUploadService.beginUpload(request.uploadId)
            responseObserver.onNext(BeginUploadResponse.newBuilder().setUploadId(request.uploadId).build())
            responseObserver.onCompleted()
        }
    }



}