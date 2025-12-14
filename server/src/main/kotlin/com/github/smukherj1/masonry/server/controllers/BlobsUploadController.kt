package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.errors.safeRun
import com.github.smukherj1.masonry.server.models.UploadStatus
import com.github.smukherj1.masonry.server.proto.BlobsUploadServiceGrpc
import com.github.smukherj1.masonry.server.proto.BlobUploadCompleteRequest
import com.github.smukherj1.masonry.server.proto.BlobUploadCompleteResponse
import com.github.smukherj1.masonry.server.proto.BlobUploadQueryRequest
import com.github.smukherj1.masonry.server.proto.BlobUploadQueryResponse
import com.github.smukherj1.masonry.server.proto.BlobUploadRequest
import com.github.smukherj1.masonry.server.proto.BlobUploadResponse
import com.github.smukherj1.masonry.server.services.BlobUploadService
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService
import java.util.UUID

@GrpcService
class BlobsUploadController(
    val blobUploadService: BlobUploadService
    ) : BlobsUploadServiceGrpc.BlobsUploadServiceImplBase() {

    val log = LoggerFactory.getLogger(BlobsUploadController::class.java)

    override fun upload(responseObserver: StreamObserver<BlobUploadResponse?>?): StreamObserver<BlobUploadRequest?>? {
        if (responseObserver == null) return null
        return object : StreamObserver<BlobUploadRequest?>{
            var uploadId: String? = null
            var serverUploadId: String? = null

            override fun onNext(u: BlobUploadRequest?) {
                if(u == null) return
                if(u.uploadId.isNullOrBlank()) return
                if(uploadId == null) {
                    uploadId = u.uploadId
                    serverUploadId = UUID.randomUUID().toString()
                } else if (!uploadId.equals(u.uploadId)) {
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("uploadId can't be changed mid-stream, got initial id $uploadId but current id is ${u.uploadId}").asRuntimeException())
                    return
                } else if(serverUploadId.isNullOrBlank()) {
                    responseObserver.onError(Status.INTERNAL.withDescription("internal upload identifier was not generated for $uploadId").asRuntimeException())
                    return
                }
                log.info("upload(): uploadId=${u.uploadId}, serverUploadId=${serverUploadId}, offset=${u.offset}, ${u.data.size()} bytes of data.")
                responseObserver.safeRun {
                    blobUploadService.upload(uploadId = u.uploadId, serverUploadId = serverUploadId.orEmpty(), offset = u.offset, data = u.data.toByteArray())
                }
            }

            override fun onCompleted() {
                log.info("upload(): uploadId=${uploadId} completed.")
                responseObserver.onNext(BlobUploadResponse.newBuilder().build())
                responseObserver.onCompleted()
            }

            override fun onError(t: Throwable?) {
                log.info("upload(): uploadId=${uploadId} error: ${t?.localizedMessage}")
                responseObserver.onError(t)
            }

        }
    }

    override fun query(request: BlobUploadQueryRequest?, responseObserver: StreamObserver<BlobUploadQueryResponse?>?)  {
        if (responseObserver == null) return
        responseObserver.safeRun {
            require(request != null) { "request must not be null" }
            require(request.uploadId.isNullOrBlank()) { "uploadId must not be null or empty" }

            val um = blobUploadService.getUpload(request.uploadId)

            if(um == null) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("upload ID ${request.uploadId} does not exist").asRuntimeException())
                return@safeRun
            }
            responseObserver.onNext(
                BlobUploadQueryResponse.newBuilder()
                    .setNextOffset(um.nextOffset)
                    .setUploadStatus(when(um.uploadStatus) {
                        UploadStatus.UNKNOWN -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_UNSPECIFIED
                        UploadStatus.ACTIVE -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_ACTIVE
                        UploadStatus.FINALIZING -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_FINALIZING
                        UploadStatus.COMPLETED -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_COMPLETED
                        UploadStatus.FAILED -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_FAILED
                    })
                .build())
            responseObserver.onCompleted()
        }
    }

    override fun complete(
        request: BlobUploadCompleteRequest?,
        responseObserver: StreamObserver<BlobUploadCompleteResponse?>?
    )  {
        requireNotNull(request) { "request must not be null" }
        if (responseObserver == null) return
        responseObserver.safeRun {
            val blobModel = blobUploadService.completeUpload(request.uploadId)
            responseObserver.onNext(
                BlobUploadCompleteResponse.newBuilder()
                    .setDigest(
                        blobModel.digest.proto
                    ).build()
            )
        }
    }



}