package datawave.microservice.annotation.common;

import java.util.function.Supplier;

import org.springframework.messaging.Message;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

public class AnnotationSupplier implements Supplier<Flux<Message<AnnotationMessage>>> {
    private final Sinks.Many<Message<AnnotationMessage>> messagingSink = Sinks.many().multicast().onBackpressureBuffer();

    public boolean send(Message<AnnotationMessage> auditMessage) {
        return messagingSink.tryEmitNext(auditMessage).isSuccess();
    }

    @Override
    public Flux<Message<AnnotationMessage>> get() {
        return messagingSink.asFlux().subscribeOn(Schedulers.boundedElastic()).share();
    }
}
