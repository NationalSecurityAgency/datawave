package datawave.microservice.query.executor;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@EmbeddedKafka
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryExecutorTest", "use-embedded-kafka"})
@ContextConfiguration(classes = QueryExecutorTest.QueryExecutorTestConfiguration.class)
public class EmbeddedKafkaQueryExecutorTest extends QueryExecutorTest {}
