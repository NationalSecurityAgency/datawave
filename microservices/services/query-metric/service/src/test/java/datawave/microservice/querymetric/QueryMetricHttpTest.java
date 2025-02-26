package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.HttpClientErrorException;

import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.microservice.querymetric.function.QueryMetricSupplier;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricHttpTest", "QueryMetricTest", "http", "hazelcast-writebehind"})
public class QueryMetricHttpTest extends QueryMetricTestBase {
    
    static {}
    
    @Autowired
    public List<QueryMetricUpdate> storedMetricUpdates;
    
    @BeforeEach
    public void setup() {
        super.setup();
        storedMetricUpdates.clear();
    }
    
    @Test
    public void RejectNonAdminUserForUpdateMetric() throws Exception {
        // @formatter:off
        assertThrows(HttpClientErrorException.Forbidden.class, () -> client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(createMetric())
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(nonAdminUser)
                .build(), QueryMetricTransportType.REST));
        // @formatter:on
    }
    
    @Test
    public void RejectNonAdminUserForUpdateMetrics() throws Exception {
        List<BaseQueryMetric> metrics = new ArrayList<>();
        metrics.add(createMetric());
        metrics.add(createMetric());
        // @formatter:off
        assertThrows(HttpClientErrorException.Forbidden.class, () -> client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(nonAdminUser)
                .build(), QueryMetricTransportType.REST));
        // @formatter:on
    }
    
    @Test
    public void AcceptAdminUserForUpdateMetric() throws Exception {
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(createMetric())
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build(), QueryMetricTransportType.REST);
        // @formatter:on
    }
    
    @Test
    public void AcceptAdminUserForUpdateMetrics() throws Exception {
        List<BaseQueryMetric> metrics = new ArrayList<>();
        metrics.add(createMetric());
        metrics.add(createMetric());
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build(), QueryMetricTransportType.REST);
        // @formatter:on
    }
    
    // Messages that arrive via http/https get placed on the message queue
    // to ensure a quick response and to maintain a single queue of work
    @Test
    public void HttpUpdateOnMessageBus() throws Exception {
        List<BaseQueryMetric> metrics = new ArrayList<>();
        metrics.add(createMetric());
        metrics.add(createMetric());
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build(), QueryMetricTransportType.REST);
        // @formatter:on
        assertEquals(2, storedMetricUpdates.size());
    }
    
    @Configuration
    @Profile("QueryMetricHttpTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryMetricHttpTestConfiguration {
        @Bean
        public List<QueryMetricUpdate> storedMetricUpdates() {
            return new ArrayList<>();
        }
        
        @Primary
        @Bean
        public QueryMetricSupplier testQueryMetricSource(@Lazy QueryMetricOperations queryMetricOperations) {
            return new QueryMetricSupplier() {
                @Override
                public boolean send(Message<QueryMetricUpdate> queryMetricUpdate) {
                    storedMetricUpdates().add(queryMetricUpdate.getPayload());
                    return true;
                }
            };
        }
    }
}
