package datawave.microservice.query.executor;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Disabled("Cannot run this test without an externally deployed RabbitMQ instance.")
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryExecutorTest", "use-rabbit"})
@ContextConfiguration(classes = QueryExecutorTest.QueryExecutorTestConfiguration.class)
public class RabbitQueryExecutorTest extends QueryExecutorTest {}
