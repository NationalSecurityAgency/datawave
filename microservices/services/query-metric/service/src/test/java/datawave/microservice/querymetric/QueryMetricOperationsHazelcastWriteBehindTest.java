package datawave.microservice.querymetric;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/*
 * Run the same tests as in the base class but with specific profile / settings
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricOperationsHazelcastWriteBehindTest", "QueryMetricTest", "MessageRouting", "hazelcast-writebehind"})
public class QueryMetricOperationsHazelcastWriteBehindTest extends QueryMetricOperationsTest {
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    @Override
    public void MetricStoredCorrectlyInCachesAndAccumulo() throws Exception {
        super.MetricStoredCorrectlyInCachesAndAccumulo();
    }
    
    @Test
    @Override
    public void MultipleMetricsStoredCorrectlyInCachesAndAccumulo() throws Exception {
        super.MultipleMetricsStoredCorrectlyInCachesAndAccumulo();
    }
    
    @Test
    @Override
    public void MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo() throws Exception {
        super.MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo();
    }
}
