package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.errors.safeRun
import com.github.smukherj1.masonry.server.proto.BeginUploadRequest
import com.github.smukherj1.masonry.server.proto.BeginUploadResponse
import com.github.smukherj1.masonry.server.proto.BlobsServiceGrpc
import com.github.smukherj1.masonry.server.proto.CompleteUploadRequest
import com.github.smukherj1.masonry.server.proto.CompleteUploadResponse
import com.github.smukherj1.masonry.server.proto.UploadRequest
import com.github.smukherj1.masonry.server.proto.UploadResponse
import com.github.smukherj1.masonry.server.services.BlobUploadService
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService

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

    override fun upload(responseObserver: StreamObserver<UploadResponse?>?): StreamObserver<UploadRequest?>? {
        if (responseObserver == null) return null
        return object : StreamObserver<UploadRequest?>{
            var uploadId: String? = null

            override fun onNext(u: UploadRequest?) {
                if(u == null) return
                if(u.uploadId.isNullOrBlank()) return
                if(uploadId == null) {
                    uploadId = u.uploadId
                } else if (!uploadId.equals(u.uploadId)) {
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("uploadId can't be changed mid-stream, got initial id $uploadId but current id is ${u.uploadId}").asRuntimeException())
                    return
                }
                log.info("upload(): uploadId=${u.uploadId}, offset=${u.offset}, ${u.data.size()} bytes of data.")
                responseObserver.safeRun {
                    blobUploadService.upload(uploadId = u.uploadId, offset = u.offset, data = u.data.toByteArray())
                }
            }

            override fun onCompleted() {
                log.info("upload(): uploadId=${uploadId} completed.")
                responseObserver.onNext(UploadResponse.newBuilder().setUploadId(uploadId).build())
                responseObserver.onCompleted()
            }

            override fun onError(t: Throwable?) {
                log.info("upload(): uploadId=${uploadId} error: ${t?.localizedMessage}")
                responseObserver.onError(t)
            }

        };
    }

    override fun completeUpload(
        request: CompleteUploadRequest?,
        responseObserver: StreamObserver<CompleteUploadResponse?>?
    ) {
        super.completeUpload(request, responseObserver)
    }



}