package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.errors.safeRun
import com.github.smukherj1.masonry.server.models.UploadStatus
import com.github.smukherj1.masonry.server.proto.BlobsServiceGrpc
import com.github.smukherj1.masonry.server.proto.CompleteUploadRequest
import com.github.smukherj1.masonry.server.proto.CompleteUploadResponse
import com.github.smukherj1.masonry.server.proto.GetUploadRequest
import com.github.smukherj1.masonry.server.proto.GetUploadResponse
import com.github.smukherj1.masonry.server.proto.UploadRequest
import com.github.smukherj1.masonry.server.proto.UploadResponse
import com.github.smukherj1.masonry.server.services.BlobUploadService
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.grpc.server.service.GrpcService
import java.util.UUID

@GrpcService
class BlobsServiceController(
    val blobUploadService: BlobUploadService
    ) : BlobsServiceGrpc.BlobsServiceImplBase() {

    val log = LoggerFactory.getLogger(BlobsServiceController::class.java)

    override fun upload(responseObserver: StreamObserver<UploadResponse?>?): StreamObserver<UploadRequest?>? {
        if (responseObserver == null) return null
        return object : StreamObserver<UploadRequest?>{
            var uploadId: String? = null
            var serverUploadId: String? = null

            override fun onNext(u: UploadRequest?) {
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
                responseObserver.onNext(UploadResponse.newBuilder().setUploadId(uploadId).build())
                responseObserver.onCompleted()
            }

            override fun onError(t: Throwable?) {
                log.info("upload(): uploadId=${uploadId} error: ${t?.localizedMessage}")
                responseObserver.onError(t)
            }

        }
    }

    override fun getUpload(request: GetUploadRequest?, responseObserver: StreamObserver<GetUploadResponse?>?) {
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
                GetUploadResponse.newBuilder()
                .setUploadId(um.uploadId)
                    .setNextOffset(um.nextOffset)
                    .setUploadStatus(when(um.uploadStatus) {
                        UploadStatus.UNKNOWN -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_UNSPECIFIED
                        UploadStatus.ACTIVE -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_ACTIVE
                        UploadStatus.COMPLETED -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_COMPLETED
                        UploadStatus.FAILED -> com.github.smukherj1.masonry.server.proto.UploadStatus.UPLOAD_STATUS_FAILED
                    })
                .build())
            responseObserver.onCompleted()
        }
    }

    override fun completeUpload(
        request: CompleteUploadRequest?,
        responseObserver: StreamObserver<CompleteUploadResponse?>?
    ) {
        super.completeUpload(request, responseObserver)
    }



}