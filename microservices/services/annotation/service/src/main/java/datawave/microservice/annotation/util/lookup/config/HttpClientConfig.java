package datawave.microservice.annotation.util.lookup.config;

import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.hc.core5.http.ConnectionReuseStrategy;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.annotation.config.AnnotationRole;
import datawave.microservice.annotation.config.ConditionalOnAnnotationRole;

/** Configuration for the HttpClient that will be used by the LookupService */
@Configuration
@ConditionalOnAnnotationRole({AnnotationRole.API, AnnotationRole.COMBINED})
public class HttpClientConfig {

    @Bean
    public CloseableHttpClient httpClient(SSLContext sslContext) {
        Registry r = RegistryBuilder.create()
                        .register("https", new SSLConnectionSocketFactory(sslContext.getSocketFactory(), null, null, NoopHostnameVerifier.INSTANCE)).build();

        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager(r);
        // manager.setDefaultMaxPerRoute(httpClientProperties.getMaxConnections());
        // manager.setMaxTotal(httpClientProperties.getMaxConnections());

        // set timeouts
        // @formatter:off
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(60 * 15 * 1000, TimeUnit.MILLISECONDS) // 15 minutes
                .setConnectionRequestTimeout(60 * 1000, TimeUnit.MILLISECONDS) // 1 minute
                .setResponseTimeout(78 * 60 * 1000, TimeUnit.MILLISECONDS)
                .build();
        // @formatter:on
        // never re-use connections
        ConnectionReuseStrategy connectionReuseStrategy = (httpRequest, httpResponse, httpContext) -> false;

        // set retry strategy to max interval of five minutes
        int maxInterval = 5 * 60 * 1000;
        return HttpClientBuilder.create().setConnectionManager(manager).setDefaultRequestConfig(requestConfig)
                        .setConnectionReuseStrategy(connectionReuseStrategy).build();
    }
}
