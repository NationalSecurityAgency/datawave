package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import datawave.microservice.querymetric.config.QueryMetricTransportType;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = MessagingTest.MessagingTestConfiguration.class)
@ActiveProfiles({"MessagingTest", "QueryMetricTest", "MessageRouting", "hazelcast-writethrough"})
public class MessagingTest extends QueryMetricTestBase {
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void sendMetricViaMessage() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .build(), QueryMetricTransportType.MESSAGE);

        ensureDataWritten(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
        // @formatter:on
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        
        ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                        BaseQueryMetricListResponse.class);
        
        assertEquals(1, metricResponse.getBody().getNumResults());
        BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
        metricAssertEquals(m, returnedMetric);
    }
    
    @Test
    public void sendMultiplePagesViaMessage() throws Exception {
        int port = webServicePort;
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        UriComponents metricUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(port).path(String.format(getMetricsUrl, queryId))
                        .build();
        HttpEntity metricRequestEntity = createRequestEntity(null, adminUser, null);
        
        int numPages = 10;
        for (int i = 0; i < numPages; i++) {
            long now = System.currentTimeMillis();
            m.addPageTime("localhost", 1000, 1000, now - 1000, now);
            
            // @formatter:off
            client.submit(new QueryMetricClient.Request.Builder()
                    .withMetric(m)
                    .withMetricType(QueryMetricType.COMPLETE)
                    .build(), QueryMetricTransportType.MESSAGE);
            // @formatter:on
            
            ResponseEntity<BaseQueryMetricListResponse> metricResponse = restTemplate.exchange(metricUri.toUri(), HttpMethod.GET, metricRequestEntity,
                            BaseQueryMetricListResponse.class);
            
            assertEquals(1, metricResponse.getBody().getNumResults());
            BaseQueryMetric returnedMetric = (BaseQueryMetric) metricResponse.getBody().getResult().get(0);
            assertEquals(i + 1, returnedMetric.getPageTimes().size());
            metricAssertEquals(m, returnedMetric);
        }
    }
}
