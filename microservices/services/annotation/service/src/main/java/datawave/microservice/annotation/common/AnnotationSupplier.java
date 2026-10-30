package datawave.microservice.annotation.common;

import java.util.function.Supplier;

import org.springframework.messaging.Message;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * Supplies REST annotation updates to the messaging stream. The bean is named {@code annotationSource} because it is the source of messages entering the
 * stream; queue consumers such as {@code accumuloAnnotationSink} are its sinks. This messaging name is separate from annotation provenance data.
 */
public class AnnotationSupplier implements Supplier<Flux<Message<AnnotationMessage>>> {
    private final Sinks.Many<Message<AnnotationMessage>> messagingSink = Sinks.many().multicast().onBackpressureBuffer();

    // Only one REST request at a time may add a message to this stream.
    // Without synchronization, simultaneous requests can cause Reactor to reject
    // a message even when messaging is healthy. The lock covers adding the message;
    // callers release it before waiting for RabbitMQ confirmation.
    public synchronized boolean send(Message<AnnotationMessage> annotationMessage) {
        return messagingSink.tryEmitNext(annotationMessage).isSuccess();
    }

    @Override
    public Flux<Message<AnnotationMessage>> get() {
        return messagingSink.asFlux().subscribeOn(Schedulers.boundedElastic()).share();
    }
}
