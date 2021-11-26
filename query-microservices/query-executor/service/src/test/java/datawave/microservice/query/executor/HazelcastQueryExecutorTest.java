package datawave.microservice.query.executor;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryExecutorTest", "use-hazelcast"})
@ContextConfiguration(classes = QueryExecutorTest.QueryExecutorTestConfiguration.class)
public class HazelcastQueryExecutorTest extends QueryExecutorTest {}
