package datawave.microservice.querymetric;

import static datawave.microservice.querymetric.QueryMetricTestBase.metricAssertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.microservice.querymetric.config.TimelyProperties;
import datawave.microservice.querymetric.function.QueryMetricSupplier;

/*
 * This class tests that a QueryMetricClient can be created and used with messaging
 * when a JWTTokenHandler is not AutoWired due to SpringBootTest.WebEnvironment.NONE
 * and ConditionalOnWebApplication in JWTConfiguration
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"NonWebApplicationMessagingTest", "QueryMetricTest"})
@ContextConfiguration(classes = {NonWebApplicationMessagingTest.QueryMetricHttpTestConfiguration.class, QueryMetricService.class})
public class NonWebApplicationMessagingTest {
    
    @Autowired
    private QueryMetricClient client;
    
    @Autowired
    private QueryMetricFactory queryMetricFactory;
    
    @Autowired
    public List<QueryMetricUpdate> storedMetricUpdates;
    
    private Map<String,String> metricMarkings;
    
    @BeforeEach
    public void setup() {
        this.metricMarkings = new HashMap<>();
        this.metricMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "A&C");
        this.storedMetricUpdates.clear();
    }
    
    /*
     * Ensure that a metric with only the queryId set will still be accepted
     */
    @Test
    public void testBareMinimumMetric() throws Exception {
        String queryId = "1111-2222-3333-4444";
        BaseQueryMetric m = queryMetricFactory.createMetric();
        m.setQueryId(queryId);
        // @formatter:off
        this.client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .build(), QueryMetricTransportType.MESSAGE);
        QueryMetricUpdate metricUpdate = this.storedMetricUpdates.stream().filter(x -> x.getMetric().getQueryId().equals(queryId)).findAny().orElse(null);
        metricAssertEquals("", metricUpdate.getMetric(), m);
    }

    @Configuration
    @Profile("NonWebApplicationMessagingTest")
    public static class QueryMetricHttpTestConfiguration {
        @Bean(name = "queryMetricCacheManager")
        public CacheManager queryMetricCacheManager() {
            return new CaffeineCacheManager();
        }

        @Bean
        public QueryMetricOperationsStats queryMetricOperationStats() {
            return new QueryMetricOperationsStats(new TimelyProperties(), null, queryMetricCacheManager(), null, null);
        }

        @Bean
        public List<QueryMetricUpdate> storedMetricUpdates() {
            return new ArrayList<>();
        }

        @Primary
        @Bean
        public QueryMetricSupplier testQueryMetricSource() {
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
