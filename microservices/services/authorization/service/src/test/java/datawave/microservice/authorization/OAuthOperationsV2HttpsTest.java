package datawave.microservice.authorization;

import javax.net.ssl.SSLContext;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;

import datawave.microservice.config.web.RestClientProperties;

// OAuthServiceTest profile to configure AuthorizationTestUserService with userMap
// http profile to use application-http.yml to test that allowedCaller not enforced for OAuth
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ActiveProfiles({"OAuthServiceTest", "httpsnotallowedcaller"})
public class OAuthOperationsV2HttpsTest extends OAuthOperationsV2TestCommon {
    
    @BeforeEach
    public void setup() {
        // notAllowedDwServer is the subject of the client cert used in this test
        // notAllowedDwServer is not on the allowedCaller list
        userMap.put(notAllowedDwUser.getDn(), notAllowedDwUser);
        // dwUser is not on the allowedCaller list
        userMap.put(dwUser.getDn(), dwUser);
        userMap.put(dwServer.getDn(), dwServer);
        OAuthRestTemplateCustomizer customizer = new OAuthRestTemplateCustomizer(sslContext, restClientProperties);
        // Disable following redirects
        restTemplateBuilder = restTemplateBuilder.additionalCustomizers(customizer);
        restTemplate = restTemplateBuilder.build(RestTemplate.class);
        testUtils = new AuthorizationTestUtils(jwtTokenHandler, restTemplate, "https", webServicePort);
    }
    
    @Test
    public void TestCodeFlowValid() throws Exception {
        super.TestCodeFlowValid(notAllowedDwUser, AUTH_TYPE.NONE);
    }
    
    @Test
    public void TestCodeFlowInvalidClientId() {
        super.TestCodeFlowInvalidClientId(notAllowedDwUser, AUTH_TYPE.NONE);
    }
    
    @Test
    public void TestCodeFlowMissingResponseType() {
        super.TestCodeFlowMissingResponseType(notAllowedDwUser, AUTH_TYPE.NONE);
    }
    
    @Test
    public void TestCodeFlowMissingRedirectUri() {
        super.TestCodeFlowMissingRedirectUri(notAllowedDwUser, AUTH_TYPE.NONE);
    }
    
    @Test
    public void TestCodeFlowWrongCode() {
        super.TestCodeFlowWrongCode();
    }
    
    @Test
    public void TestCodeFlowRightCodeWrongOtherParameters() throws Exception {
        super.TestCodeFlowRightCodeWrongOtherParameters(notAllowedDwUser, AUTH_TYPE.NONE);
    }
    
    @Test
    public void TestCodeFlowUseCodeTwice() throws Exception {
        super.TestCodeFlowUseCodeTwice(notAllowedDwUser, AUTH_TYPE.NONE);
    }
    
    /*
     * Only used for the OAuth tests so that we can ignore redirect responses and check the response values at each step of the OAuth process
     */
    public class OAuthRestTemplateCustomizer implements RestTemplateCustomizer {
        
        private final SSLContext sslContext;
        private final int maxConnectionsTotal;
        private final int maxConnectionsPerRoute;
        
        public OAuthRestTemplateCustomizer(SSLContext sslContext, RestClientProperties restClientProperties) {
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
            httpClientBuilder.disableRedirectHandling();
            // TODO: We're allowing all hosts, since the cert presented by the service we're calling likely won't match its hostname (e.g., a docker host name)
            // Instead, we could list the expected cert as a property (or use our server cert), and verify that the presented name matches.
            return httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
    }
}
