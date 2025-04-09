package datawave.microservice.config;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.cloud.bus.PathServiceMatcher;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.config.metrics.MetricsConfigurationProperties;
import datawave.microservice.config.web.RestClientProperties;
import datawave.security.authorization.JWTTokenHandler;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

/**
 * Configures default beans needed by DATAWAVE microservices.
 */
@Configuration
public class DatawaveMicroserviceConfig {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Bean
    @ConditionalOnMissingBean(search = SearchStrategy.CURRENT)
    public RestClientProperties restClientProperties() {
        return new RestClientProperties();
    }
    
    @Bean
    public MetricsConfigurationProperties metricsConfigurationProperties() {
        return new MetricsConfigurationProperties();
    }
    
    @Bean
    @Profile("nomessaging")
    public ServiceMatcher serviceMatcher() {
        // This matcher is used to deal with spring bus events. If we're running with the "nomessaging" profile, that means
        // we're not using a message bus and therefore no ServiceMatcher will be created. However, we still reference one
        // for internal use, so we make a dummy for that case.
        return new PathServiceMatcher(new AntPathMatcher(), "invalid");
    }
    
    @Bean
    @Qualifier("serverUserDetailsSupplier")
    @ConditionalOnWebApplication
    public Supplier<DatawaveUserDetails> serverUserDetailsSupplier(JWTTokenHandler jwtTokenHandler,
                    @Qualifier("outboundNettySslContext") SslContext nettySslContext, WebClient.Builder webClientBuilder,
                    @Value("${datawave.authorization.uri:https://authorization:8443/authorization/v1/authorize}") String authorizationUri) {
        // @formatter:off
        TcpClient timeoutClient = TcpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                .doOnConnected(con -> con.addHandlerLast(new ReadTimeoutHandler(6)))
                .secure(sslContextSpec -> sslContextSpec.sslContext(nettySslContext));
        WebClient webClient = webClientBuilder.clone().clientConnector(new ReactorClientHttpConnector(HttpClient.from(timeoutClient))).build();
        // @formatter:on
        
        return new Supplier<>() {
            DatawaveUserDetails serverUserDetails = null;
            
            @Override
            public DatawaveUserDetails get() {
                synchronized (this) {
                    if (serverUserDetails == null || (System.currentTimeMillis() > (this.serverUserDetails.getCreationTime() + TimeUnit.DAYS.toMillis(1)))) {
                        try {
                            // @formatter:off
                            WebClient.ResponseSpec response = webClient.get()
                                    .uri(authorizationUri)
                                    .retrieve();
                            // @formatter:on
                            
                            String jwtString = response.bodyToMono(String.class).block(Duration.ofSeconds(30));
                            
                            serverUserDetails = new DatawaveUserDetails(jwtTokenHandler.createUsersFromToken(jwtString), System.currentTimeMillis());
                        } catch (Exception e) {
                            log.warn("Unable to create server proxied user details via {}", authorizationUri);
                        }
                    }
                }
                return serverUserDetails;
            }
        };
    }
}
