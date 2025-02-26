package datawave.microservice.config.web.filter;

import static datawave.microservice.config.web.Constants.OPERATION_TIME_MS_HEADER;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;
import static datawave.microservice.config.web.Constants.RESPONSE_ORIGIN_HEADER;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import reactor.core.publisher.Mono;

public class ResponseHeaderWebFilter implements WebFilter, Ordered {
    
    private final String origin;
    
    // Make this filter near the highest priority so that we're sure to capture the response time appropriately.
    private int order = Ordered.HIGHEST_PRECEDENCE + 1;
    
    public ResponseHeaderWebFilter(String systemName) {
        String origin;
        try {
            origin = systemName + " / " + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            origin = systemName + " / UNKNOWN";
        }
        this.origin = origin;
    }
    
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (exchange.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE) == null) {
            exchange.getAttributes().put(REQUEST_START_TIME_NS_ATTRIBUTE, System.nanoTime());
        }
        exchange.getResponse().beforeCommit(() -> {
            HttpHeaders headers = exchange.getResponse().getHeaders();
            headers.set(RESPONSE_ORIGIN_HEADER, origin);
            if (!headers.containsKey(OPERATION_TIME_MS_HEADER)) {
                @SuppressWarnings("ConstantConditions")
                long startTimeNanos = exchange.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE);
                long operationTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                headers.add(OPERATION_TIME_MS_HEADER, Long.toString(operationTimeMillis));
            }
            return Mono.empty();
        });
        return chain.filter(exchange);
    }
    
    @Override
    public int getOrder() {
        return order;
    }
    
    public void setOrder(int order) {
        this.order = order;
    }
}
