package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.proto.BeginUploadRequest
import com.github.smukherj1.masonry.server.proto.BeginUploadResponse
import com.github.smukherj1.masonry.server.proto.BlobsServiceGrpc
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService

@GrpcService
class BlobsServiceController : BlobsServiceGrpc.BlobsServiceImplBase() {

    val log = LoggerFactory.getLogger(BlobsServiceController::class.java)

    override fun beginUpload(request: BeginUploadRequest?, responseObserver: StreamObserver<BeginUploadResponse?>?) {
        log.info("beginUpload(): uploadId=${request?.uploadId}")
        if(responseObserver == null) return
        responseObserver.onNext(BeginUploadResponse.newBuilder().setUploadId(request?.uploadId).build())
        responseObserver.onCompleted()
    }


}