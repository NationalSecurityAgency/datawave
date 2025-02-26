package datawave.microservice.audit.common;

import java.util.function.Supplier;

import org.springframework.messaging.Message;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

public class AuditMessageSupplier implements Supplier<Flux<Message<AuditMessage>>> {
    private final Sinks.Many<Message<AuditMessage>> messagingSink = Sinks.many().multicast().onBackpressureBuffer();
    
    public boolean send(Message<AuditMessage> auditMessage) {
        return messagingSink.tryEmitNext(auditMessage).isSuccess();
    }
    
    @Override
    public Flux<Message<AuditMessage>> get() {
        return messagingSink.asFlux().subscribeOn(Schedulers.boundedElastic()).share();
    }
}
