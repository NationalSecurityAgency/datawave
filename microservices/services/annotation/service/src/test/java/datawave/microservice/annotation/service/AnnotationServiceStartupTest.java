package datawave.microservice.annotation.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterProperties;
import datawave.microservice.annotation.writers.file.config.FileAnnotationWriterConfig;
import reactor.core.Disposable;

/**
 * Starts a Spring context for the combined role with Accumulo writing and file fallback enabled, and log and dump writers disabled.
 * Checks that the expected controller, publisher, writer, and consumer beans are present.
 *
 * Uses in-memory Accumulo and a mocked lookup service. Calls the controller directly to submit an annotation, then simulates
 * RabbitMQ accepting the publication by completing its confirmation future. Checks that the write succeeds after that confirmation.
 *
 * This test does not use a live RabbitMQ broker or verify that a queue consumer saves the annotation to Accumulo.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {AnnotationServiceConfig.class, AnnotationSerializerConfiguration.class, AccumuloAnnotationWriterConfig.class,
        FileAnnotationWriterConfig.class, AnnotationControllerV1.class, AnnotationServiceStartupTest.AnnotationServiceStartupTestConfiguration.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"annotation-combined", "AnnotationServiceStartupTest", "accumulo-enabled", "file-enabled"})
public class AnnotationServiceStartupTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AnnotationControllerV1 annotationController;

    @Autowired
    private AnnotationSupplier annotationSource;

    private Disposable brokerSubscription;

    @BeforeEach
    void simulateHealthyBrokerProducerConfirms() {
        brokerSubscription = annotationSource.get().subscribe(sent -> sent.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, CorrelationData.class)
                        .getFuture().set(new CorrelationData.Confirm(true, null)));
    }

    @AfterEach
    void disconnectBroker() {
        brokerSubscription.dispose();
    }

    @Test
    void testProductionShapeWriterTopologyBootsCleanly() {
        assertTrue(context.containsBean("accumuloAnnotationWriter"), "expected accumuloAnnotationWriter (enabled in shipped annotation.yml)");
        assertTrue(context.containsBean("accumuloAnnotationSink"), "expected accumuloAnnotationSink (enabled in shipped annotation.yml)");
        assertTrue(context.containsBean("fileAnnotationWriter"), "expected fileAnnotationWriter (enabled in shipped annotation.yml)");
        assertTrue(context.containsBean("fileAnnotationWriterProperties"), "expected fileAnnotationWriterProperties (enabled in shipped annotation.yml)");
        assertTrue(context.containsBean("annotationSource"), "expected the annotationSource message producer to be present");
        assertTrue(context.containsBean("annotationControllerV1"), "expected the annotation write/read controller to be present");

        // log and dump writers are disabled in the shipped annotation.yml topology; confirm they stay absent here too
        assertFalse(context.containsBean("logAnnotationWriter"), "logAnnotationWriter is disabled in the shipped topology and should not be present");
        assertFalse(context.containsBean("dumpAnnotationWriter"), "dumpAnnotationWriter is disabled in the shipped topology and should not be present");
    }

    @Test
    void testAuthorizedWriteReachesProducerConfirmedAcceptanceBoundary() {
        Annotation annotation = AnnotationTestDataUtil.generateTestAnnotation();

        java.util.Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isPresent(), "an authorized write should succeed once producer-confirm ack is received, per the Q01 acceptance boundary");
    }

    @Configuration
    public static class AnnotationServiceStartupTestConfiguration {
        @Bean
        public AccumuloConnectionFactory testAccumuloConnectionFactory(AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties) throws Exception {
            AccumuloAnnotationWriterProperties.Accumulo accumulo = accumuloAnnotationWriterProperties.getAccumuloConfig();
            return new MockInMemoryAccumuloConnectionFactory(accumulo.getUsername(), accumulo.getInstanceName());
        }

        @Bean
        public LookupService lookupService() {
            // the lookup subsystem (T11-T13) is exercised separately; this write acceptance test bypasses lookup entirely
            // by calling writeAnnotation(Annotation) directly with a pre-localized annotation.
            return Mockito.mock(LookupService.class);
        }
    }

    private static class MockInMemoryAccumuloConnectionFactory implements AccumuloConnectionFactory {
        private final org.apache.accumulo.core.client.AccumuloClient accumuloClient;

        public MockInMemoryAccumuloConnectionFactory(String username, String instanceName) throws Exception {
            InMemoryInstance inMemoryInstance = new InMemoryInstance(instanceName);
            this.accumuloClient = new InMemoryAccumuloClient(username, inMemoryInstance);
            accumuloClient.securityOperations().changeUserAuthorizations(username, new org.apache.accumulo.core.security.Authorizations("PUBLIC", "PRIVATE"));
        }

        @Override
        public org.apache.accumulo.core.client.AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, Priority priority,
                        Map<String,String> trackingMap) throws Exception {
            return accumuloClient;
        }

        @Override
        public org.apache.accumulo.core.client.AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, String poolName, Priority priority,
                        Map<String,String> trackingMap) throws Exception {
            return accumuloClient;
        }

        @Override
        public void returnClient(org.apache.accumulo.core.client.AccumuloClient client) {

        }

        @Override
        public String report() {
            return null;
        }

        @Override
        public List<ConnectionPool> getConnectionPools() {
            return null;
        }

        @Override
        public int getConnectionUsagePercent() {
            return 0;
        }

        @Override
        public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
            return new java.util.HashMap<>();
        }

        @Override
        public void close() throws Exception {

        }
    }
}
