package com.github.smukherj1.masonry.server.errors

import io.grpc.Status
import io.grpc.stub.StreamObserver

fun <T> StreamObserver<T>.safeRun(block: () -> Unit) {
    try {
        block()
    } catch (e: IllegalArgumentException) {
        this.onError(Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException())
    } catch (e: IllegalStateException) {
        this.onError(Status.FAILED_PRECONDITION.withDescription(e.message).asRuntimeException())
    } catch (e: Exception) {
        this.onError(Status.INTERNAL.withDescription(e.message).asRuntimeException())
    }
}
