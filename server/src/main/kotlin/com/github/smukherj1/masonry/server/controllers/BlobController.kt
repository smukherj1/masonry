package com.github.smukherj1.masonry.server.controllers

import com.github.smukherj1.masonry.server.errors.safeRun
import com.github.smukherj1.masonry.server.models.DigestModel
import com.github.smukherj1.masonry.server.proto.BlobDownloadRequest
import com.github.smukherj1.masonry.server.proto.BlobDownloadResponse
import com.github.smukherj1.masonry.server.proto.BlobQueryRequest
import com.github.smukherj1.masonry.server.proto.BlobQueryResponse
import com.github.smukherj1.masonry.server.proto.BlobServiceGrpc
import com.github.smukherj1.masonry.server.services.BlobService
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.stub.StreamObserver
import org.springframework.grpc.server.service.GrpcService
import java.time.ZoneOffset

@GrpcService
class BlobController(
    val blobService: BlobService
) : BlobServiceGrpc.BlobServiceImplBase() {

    override fun download(request: BlobDownloadRequest?, responseObserver: StreamObserver<BlobDownloadResponse?>?) {
        requireNotNull(request) { "request must not be null" }
        if(responseObserver == null) return
        blobService.download(DigestModel(request.digest), request.offset, request.limit
        ) { offset, data ->
            responseObserver.safeRun {
                responseObserver.onNext(
                    BlobDownloadResponse.newBuilder()
                        .setOffset(offset)
                        .setData(ByteString.copyFrom(data))
                        .build()
                )
            }
        }
        responseObserver.onCompleted()
    }

    override fun query(request: BlobQueryRequest?, responseObserver: StreamObserver<BlobQueryResponse?>?) {
        requireNotNull(request) { "request must not be null" }
        if(responseObserver == null) return
        responseObserver.safeRun {
            val blob = blobService.query(DigestModel(request.digest))
            responseObserver.onNext(
                BlobQueryResponse.newBuilder()
                    .setCreatedAt(
                        Timestamp.newBuilder()
                            .setSeconds(blob.createdAt.toEpochSecond(
                                ZoneOffset.UTC
                            ))
                            .build()
                    )
                    .build()
            )
            responseObserver.onCompleted()
        }
    }
}