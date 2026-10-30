package datawave.microservice.annotationCache;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/** Factory and Spring connection-property helper for isolated RabbitMQ Testcontainers. */
abstract class RabbitMqIntegrationTestSupport {
    private static final String RABBITMQ_IMAGE = "rabbitmq";
    private static final String RABBITMQ_TAG = "3.13.7-alpine";
    private static final String RABBITMQ_USERNAME = "annotation-test";
    private static final String RABBITMQ_PASSWORD = "annotation-test-password";

    static RabbitMQContainer newRabbitContainer() {
        return new RabbitMQContainer(DockerImageName.parse(RABBITMQ_IMAGE + ":" + RABBITMQ_TAG)).withAdminPassword(RABBITMQ_PASSWORD)
                        .withEnv("RABBITMQ_DEFAULT_USER", RABBITMQ_USERNAME);
    }

    static void registerRabbitProperties(RabbitMQContainer rabbit, DynamicPropertyRegistry properties) {
        properties.add("spring.rabbitmq.host", rabbit::getHost);
        properties.add("spring.rabbitmq.port", rabbit::getAmqpPort);
        properties.add("spring.rabbitmq.username", () -> RABBITMQ_USERNAME);
        properties.add("spring.rabbitmq.password", () -> RABBITMQ_PASSWORD);
    }
}
