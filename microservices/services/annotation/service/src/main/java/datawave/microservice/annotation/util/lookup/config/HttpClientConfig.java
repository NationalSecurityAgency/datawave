package datawave.microservice.annotation.util.lookup.config;

import javax.net.ssl.SSLContext;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configuration for the HttpClient that will be used by the LookupService */
@Configuration
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
                .setConnectTimeout(60 * 15 * 1000) // 15 minutes
                .setConnectionRequestTimeout(60 * 1000) // 1 minute
                .setSocketTimeout(78 * 60 * 1000)
                .build();
        // @formatter:on
        // never re-use connections
        ConnectionReuseStrategy connectionReuseStrategy = (httpResponse, httpContext) -> false;

        // set retry strategy to max interval of five minutes
        int maxInterval = 5 * 60 * 1000;
        return HttpClientBuilder.create().setSSLContext(sslContext).setConnectionManager(manager).setDefaultRequestConfig(requestConfig)
                        .setConnectionReuseStrategy(connectionReuseStrategy).build();
    }
}
