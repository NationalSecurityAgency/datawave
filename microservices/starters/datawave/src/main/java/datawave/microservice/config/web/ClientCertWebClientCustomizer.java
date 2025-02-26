package datawave.microservice.config.web;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer;
import org.springframework.core.annotation.Order;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;

import io.netty.handler.ssl.SslContext;
import reactor.netty.http.client.HttpClient;

/**
 * Customizes the Spring-provided {@link org.springframework.web.reactive.function.client.WebClient.Builder} in order to supply an {@link SslContext} that will
 * provide a client certificate to the remote server if one is requested.
 */
@Component
@Order(100) // execute this after standard customizers so we're sure to overwrite the client connector
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "server.outbound-ssl.enabled", matchIfMissing = true)
public class ClientCertWebClientCustomizer implements WebClientCustomizer {
    private final SslContext sslContext;
    private final boolean wiretap;
    
    public ClientCertWebClientCustomizer(@Qualifier("outboundNettySslContext") SslContext sslContext,
                    @Value("${reactor.netty.http.client.wiretap:false}") boolean wiretap) {
        this.sslContext = sslContext;
        this.wiretap = wiretap;
    }
    
    @Override
    public void customize(org.springframework.web.reactive.function.client.WebClient.Builder webClientBuilder) {
        // @formatter:off
        HttpClient httpClient = HttpClient.create()
                .secure(sslContextSpec -> sslContextSpec.sslContext(sslContext))
                .wiretap(wiretap);
        ReactorClientHttpConnector clientHttpConnector = new ReactorClientHttpConnector(httpClient);
        webClientBuilder.clientConnector(clientHttpConnector);
        // @formatter:on
    }
}
