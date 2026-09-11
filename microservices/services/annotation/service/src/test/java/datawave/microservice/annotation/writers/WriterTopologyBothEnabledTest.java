package datawave.microservice.annotation.writers;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.microservice.annotation.common.AnnotationConsumer;
import datawave.microservice.annotation.common.config.AccumuloConfiguration;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterProperties;
import datawave.microservice.annotation.writers.log.config.LogAnnotationWriterProperties;

/**
 * Verifies that when more than one {@link AnnotationWriter} bean is present in the context (log and Accumulo enabled together), each consumer's writer
 * dependency is disambiguated by an explicit {@link Qualifier} rather than relying on incidental parameter-name-to-bean-name matching. Without the qualifiers
 * added for T08, Spring would either fail to start with a {@code NoUniqueBeanDefinitionException} or - worse - silently wire the wrong writer to a consumer
 * whose parameter name happens not to match any candidate bean name (as was the case for {@code logAnnotationSink}).
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {LogAnnotationWriterProperties.class, AccumuloAnnotationWriterConfig.class, AccumuloConfiguration.class,
        AnnotationSerializerConfiguration.class, WriterTopologyBothEnabledTest.WriterTopologyTestConfiguration.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "spring.main.allow-bean-definition-overriding=true")
@ActiveProfiles({"WriterTopologyBothEnabledTest", "log-enabled", "accumulo-enabled"})
public class WriterTopologyBothEnabledTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    @Qualifier("logAnnotationWriter")
    private AnnotationWriter logAnnotationWriter;

    @Autowired
    @Qualifier("accumuloAnnotationWriter")
    private AnnotationWriter accumuloAnnotationWriter;

    @Test
    public void testBothWriterAndSinkBeansPresent() {
        assertTrue(context.containsBean("logAnnotationWriter"), "expected logAnnotationWriter to be present");
        assertTrue(context.containsBean("logAnnotationSink"), "expected logAnnotationSink to be present");
        assertTrue(context.containsBean("accumuloAnnotationWriter"), "expected accumuloAnnotationWriter to be present");
        assertTrue(context.containsBean("accumuloAnnotationSink"), "expected accumuloAnnotationSink to be present");
    }

    @Test
    public void testLogSinkIsWiredToLogWriter() {
        AnnotationConsumer logAnnotationSink = (AnnotationConsumer) context.getBean("logAnnotationSink");
        Object wiredWriter = ReflectionTestUtils.getField(logAnnotationSink, "annotationWriter");
        assertSame(logAnnotationWriter, wiredWriter, "expected logAnnotationSink to be wired to the logAnnotationWriter bean, not some other writer");
    }

    @Test
    public void testAccumuloSinkIsWiredToAccumuloWriter() {
        AnnotationConsumer accumuloAnnotationSink = (AnnotationConsumer) context.getBean("accumuloAnnotationSink");
        Object wiredWriter = ReflectionTestUtils.getField(accumuloAnnotationSink, "annotationWriter");
        assertSame(accumuloAnnotationWriter, wiredWriter,
                        "expected accumuloAnnotationSink to be wired to the accumuloAnnotationWriter bean, not some other writer");
    }

    @Configuration
    public static class WriterTopologyTestConfiguration {
        @Bean
        public AccumuloConnectionFactory accumuloConnectionFactory(AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties) throws Exception {
            AccumuloAnnotationWriterProperties.Accumulo accumulo = accumuloAnnotationWriterProperties.getAccumuloConfig();
            return new MockInMemoryAccumuloConnectionFactory(accumulo.getUsername(), accumulo.getInstanceName());
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
