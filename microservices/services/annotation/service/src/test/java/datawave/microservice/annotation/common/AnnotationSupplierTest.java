package datawave.microservice.annotation.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.springframework.messaging.support.GenericMessage;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import reactor.core.Disposable;

class AnnotationSupplierTest {
    @Test
    void overlappingRequestsAreAllDeliveredWithoutRetrying() throws Exception {
        AnnotationSupplier supplier = new AnnotationSupplier();
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        CountDownLatch received = new CountDownLatch(64);
        Disposable subscription = supplier.get().subscribe(message -> {
            // Model a downstream publisher that takes time to process an emission.
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            delivered.add(message.getPayload().getAnnotationMessageId());
            received.countDown();
        });
        ExecutorService requests = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Boolean>> accepted = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                AnnotationMessage message = AnnotationMessage.newBuilder().setAnnotationMessageId("concurrent-" + i).build();
                accepted.add(requests.submit(() -> {
                    start.await();
                    return supplier.send(new GenericMessage<>(message));
                }));
            }
            start.countDown();
            for (Future<Boolean> result : accepted) {
                assertThat(result.get(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(delivered).hasSize(64);
        } finally {
            requests.shutdownNow();
            subscription.dispose();
        }
    }
}
