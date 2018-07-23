package datawave.microservice.config.web;

import javax.net.ssl.SSLContext;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * Customizes Spring {@link RestTemplate} instances by using our configured SSL certificate to present a client certificate whenever asked by remote services.
 */
@Component
public class ClientCertRestTemplateCustomizer implements RestTemplateCustomizer {
    private final SSLContext sslContext;
    private final int maxConnectionsTotal;
    private final int maxConnectionsPerRoute;
    
    @Autowired
    public ClientCertRestTemplateCustomizer(SSLContext sslContext, RestClientProperties restClientProperties) {
        this.sslContext = sslContext;
        this.maxConnectionsTotal = restClientProperties.getMaxConnectionsTotal();
        this.maxConnectionsPerRoute = restClientProperties.getMaxConnectionsPerRoute();
    }
    
    @Override
    public void customize(RestTemplate restTemplate) {
        restTemplate.setRequestFactory(clientHttpRequestFactory());
    }
    
    protected ClientHttpRequestFactory clientHttpRequestFactory() {
        HttpClient httpClient = customizeHttpClient(HttpClients.custom(), sslContext).build();
        return new HttpComponentsClientHttpRequestFactory(httpClient);
    }
    
    protected HttpClientBuilder customizeHttpClient(HttpClientBuilder httpClientBuilder, SSLContext sslContext) {
        if (sslContext != null) {
            httpClientBuilder.setSSLContext(sslContext);
        }
        httpClientBuilder.setMaxConnTotal(maxConnectionsTotal);
        httpClientBuilder.setMaxConnPerRoute(maxConnectionsPerRoute);
        // TODO: We're allowing all hosts, since the cert presented by the service we're calling likely won't match its hostname (e.g., a docker host name)
        // Instead, we could list the expected cert as a property (or use our server cert), and verify that the presented name matches.
        return httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    }
}
