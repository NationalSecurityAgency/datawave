package datawave.microservice.annotation.writers.accumulo;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationSourcesEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.data.AnnotationUpdateException;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterProperties;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ActiveProfiles({"AccumuloAnnotationWriterTest", "accumulo-enabled"})
public class AccumuloAnnotationWriterTest {
    private static final String annotationTableName = "annotation";
    private static final String annotationSourceTableName = "annotationSource";

    @Autowired
    private AccumuloAnnotationWriter accumuloAnnotationWriter;

    @Autowired
    private AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties;

    @Autowired
    AccumuloConnectionFactory accumuloConnectionFactory;

    @Autowired
    AccumuloAnnotationSerializer annotationSerializer;

    @Autowired
    AccumuloAnnotationSourceSerializer annotationSourceSerializer;

    @Autowired
    private ApplicationContext context;

    private final Map<String,String> trackingMap = new HashMap<>();

    @Test
    public void testBeansPresent() {
        assertTrue(context.containsBean("accumuloAnnotationSink"), "expected accumuloAnnotationSink to be present");
        assertTrue(context.containsBean("accumuloAnnotationWriter"), "expected accumuloAnnotationWriter to be present");
    }

    @Test
    public void testInit() throws Exception {
        AccumuloClient accumuloClient = accumuloConnectionFactory.getClient(AccumuloAnnotationWriter.USER_DN, AccumuloAnnotationWriter.EMPTY_PROXY_SERVERS,
                        AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
        try {
            if (accumuloClient.tableOperations().exists(annotationTableName))
                accumuloClient.tableOperations().delete(annotationTableName);

            if (accumuloClient.tableOperations().exists(annotationSourceTableName))
                accumuloClient.tableOperations().delete(annotationSourceTableName);

            assertFalse(accumuloClient.tableOperations().exists(annotationTableName), annotationTableName + " already exists before test");
            assertFalse(accumuloClient.tableOperations().exists(annotationSourceTableName), annotationSourceTableName + " already exists before test");

            // test initialization
            accumuloAnnotationWriter = new AccumuloAnnotationWriter(accumuloConnectionFactory, accumuloAnnotationWriterProperties, annotationSerializer,
                            annotationSourceSerializer);

            assertTrue(accumuloClient.tableOperations().exists(annotationTableName), annotationTableName + " doesn't exist after test");
            assertTrue(accumuloClient.tableOperations().exists(annotationSourceTableName), annotationSourceTableName + " doesn't exist after test");
        } finally {
            accumuloConnectionFactory.returnClient(accumuloClient);
        }
    }

    @Test
    public void testAnnotationWriter() throws Exception {
        AccumuloClient accumuloClient = accumuloConnectionFactory.getClient(AccumuloAnnotationWriter.USER_DN, AccumuloAnnotationWriter.EMPTY_PROXY_SERVERS,
                        AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
        try {
            accumuloClient.tableOperations().deleteRows(annotationTableName, null, null);
            accumuloClient.tableOperations().deleteRows(annotationSourceTableName, null, null);

            // write the annotation and source.
            Annotation partialAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
            Annotation testAnnotation = AnnotationUtils.injectAllHashes(partialAnnotation);
            Optional<Annotation> result = accumuloAnnotationWriter.write(testAnnotation);
            assertFalse(result.isEmpty());
            Annotation expectedAnnotation = result.get();

            AnnotationDataAccess dao = accumuloAnnotationWriter.getDataAccess();

            // validate some preconditions
            assertNotNull(expectedAnnotation.getSource());
            assertNotNull(expectedAnnotation.getAnalyticSourceHash());

            // do some debugging
            MockInMemoryAccumuloConnectionFactory mock = (MockInMemoryAccumuloConnectionFactory) accumuloConnectionFactory;
            mock.dumpTableList();
            mock.dumpTable(annotationTableName);
            mock.dumpTable(annotationSourceTableName);

            // check that the annotations match.
            Optional<Annotation> observedAnnotationRef = dao.getAnnotation(expectedAnnotation.getShard(), expectedAnnotation.getDataType(),
                            expectedAnnotation.getUid(), expectedAnnotation.getAnnotationId());
            assertFalse(observedAnnotationRef.isEmpty(),
                            String.format("could not retrieve the target annotation: '%s/%s/%s:(%s)'", expectedAnnotation.getShard(),
                                            expectedAnnotation.getDataType(), expectedAnnotation.getDocumentId(), expectedAnnotation.getAnnotationId()));
            // asserts that the annotation and segments are equal
            assertAnnotationsEqual(expectedAnnotation, observedAnnotationRef.get());

            // check that the sources match
            Optional<AnnotationSource> observedAnnotationSourceRef = dao.getAnnotationSource(expectedAnnotation.getAnalyticSourceHash());
            assertFalse(observedAnnotationSourceRef.isEmpty(),
                            String.format("could not retrieve the target annotation source: '%s'", expectedAnnotation.getAnalyticSourceHash()));
            // asserts that the sources (and hashes) are equal
            assertAnnotationSourcesEqual(expectedAnnotation.getSource(), observedAnnotationSourceRef.get());
        } finally {
            accumuloConnectionFactory.returnClient(accumuloClient);
        }
    }

    @Test
    public void testAnnotationWriterUpdate() throws Exception {
        AccumuloClient accumuloClient = accumuloConnectionFactory.getClient(AccumuloAnnotationWriter.USER_DN, AccumuloAnnotationWriter.EMPTY_PROXY_SERVERS,
                        AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
        try {
            accumuloClient.tableOperations().deleteRows(annotationTableName, null, null);
            accumuloClient.tableOperations().deleteRows(annotationSourceTableName, null, null);

            // write the original annotation first, this is the target of the update.
            Annotation partialAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
            Annotation testAnnotation = AnnotationUtils.injectAllHashes(partialAnnotation);
            Optional<Annotation> originalResult = accumuloAnnotationWriter.write(testAnnotation);
            assertFalse(originalResult.isEmpty());
            Annotation originalAnnotation = originalResult.get();

            // build a replacement annotation that references the original via the UPDATE_REFERENCE metadata key, the way
            // AnnotationControllerV1#updateAnnotation does.
            Annotation rawUpdate = partialAnnotation.toBuilder().clearAnnotationId().clearSegments().addAllSegments(partialAnnotation.getSegmentsList().stream()
                            .map(s -> s.toBuilder().clearSegmentHash().build()).collect(java.util.stream.Collectors.toList())).build();
            Annotation update = AnnotationUtils.injectUpdateReference(rawUpdate, originalAnnotation.getAnnotationId());

            Optional<Annotation> updateResult = accumuloAnnotationWriter.write(update);
            assertFalse(updateResult.isEmpty());
            Annotation writtenUpdate = updateResult.get();

            // the update reference metadata must be preserved through the write, and the resulting annotation id must be
            // different from the original since the metadata (and therefore the content hash) differs.
            assertEquals(originalAnnotation.getAnnotationId(), writtenUpdate.getMetadataMap().get(AnnotationUtils.UPDATE_REFERENCE),
                            "expected the written update to reference the original annotation id");

            AnnotationDataAccess dao = accumuloAnnotationWriter.getDataAccess();

            // the original annotation must still be present and unmodified.
            Optional<Annotation> observedOriginal = dao.getAnnotation(originalAnnotation.getShard(), originalAnnotation.getDataType(),
                            originalAnnotation.getUid(), originalAnnotation.getAnnotationId());
            assertFalse(observedOriginal.isEmpty(), "expected the original annotation to remain in the store after the update");
            assertAnnotationsEqual(originalAnnotation, observedOriginal.get());

            // the update must be independently retrievable and carry the reference to the original.
            Optional<Annotation> observedUpdate = dao.getAnnotation(writtenUpdate.getShard(), writtenUpdate.getDataType(), writtenUpdate.getUid(),
                            writtenUpdate.getAnnotationId());
            assertFalse(observedUpdate.isEmpty(), "expected the update annotation to be retrievable from the store");
            assertAnnotationsEqual(writtenUpdate, observedUpdate.get());
        } finally {
            accumuloConnectionFactory.returnClient(accumuloClient);
        }
    }

    @Test
    public void testAnnotationWriterUpdateTargetNotFound() throws Exception {
        AccumuloClient accumuloClient = accumuloConnectionFactory.getClient(AccumuloAnnotationWriter.USER_DN, AccumuloAnnotationWriter.EMPTY_PROXY_SERVERS,
                        AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
        try {
            accumuloClient.tableOperations().deleteRows(annotationTableName, null, null);
            accumuloClient.tableOperations().deleteRows(annotationSourceTableName, null, null);

            Annotation partialAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
            Annotation update = AnnotationUtils.injectUpdateReference(partialAnnotation, "nonexistentAnnotationId");

            assertThrows(AnnotationUpdateException.class, () -> accumuloAnnotationWriter.write(update),
                            "expected an update referencing a nonexistent target annotation id to fail");
        } finally {
            accumuloConnectionFactory.returnClient(accumuloClient);
        }
    }

    @Configuration
    @Profile("AccumuloAnnotationWriterTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AccumuloAnnotationWriterTestConfiguration {
        @Bean
        public AccumuloConnectionFactory accumuloConnectionFactory(AccumuloAnnotationWriterProperties accumuloAnnotationWriterProperties) throws Exception {
            AccumuloAnnotationWriterProperties.Accumulo accumulo = accumuloAnnotationWriterProperties.getAccumuloConfig();
            return new MockInMemoryAccumuloConnectionFactory(accumulo.getUsername(), accumulo.getInstanceName());
        }
    }

    @Slf4j
    private static class MockInMemoryAccumuloConnectionFactory implements AccumuloConnectionFactory {
        private final AccumuloClient accumuloClient;

        public MockInMemoryAccumuloConnectionFactory(String username, String instanceName) throws Exception {
            InMemoryInstance inMemoryInstance = new InMemoryInstance(instanceName);
            this.accumuloClient = new InMemoryAccumuloClient(username, inMemoryInstance);
            accumuloClient.securityOperations().changeUserAuthorizations(username, new Authorizations("PUBLIC", "PRIVATE"));
        }

        public void dumpTableList() {
            try {
                Set<String> tableList = accumuloClient.tableOperations().list();
                log.debug("*************** table list ********************");
                tableList.forEach(t -> log.info("Table: {}}", t));
            } catch (Exception e) {
                log.error("Error dumping tables", e);
            }
        }

        public void dumpTable(String table) {
            try {
                String accumuloUser = accumuloClient.whoami();
                final Authorizations auths = accumuloClient.securityOperations().getUserAuthorizations(accumuloUser);
                Scanner scanner = accumuloClient.createScanner(table, auths);
                Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
                log.debug("*************** " + table + " ********************");
                while (iterator.hasNext()) {
                    Map.Entry<Key,Value> entry = iterator.next();
                    log.debug("key: {}; value length: {}", entry.getKey(), entry.getValue().getSize());
                }
                scanner.close();
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException(e);
            } catch (TableNotFoundException e) {
                throw new RuntimeException("TableNotFoundException: ", e);
            }
        }

        @Override
        public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, Priority priority, Map<String,String> trackingMap) throws Exception {
            return accumuloClient;
        }

        @Override
        public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, String poolName, Priority priority, Map<String,String> trackingMap)
                        throws Exception {
            return accumuloClient;
        }

        @Override
        public void returnClient(AccumuloClient client) {

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
            return new HashMap<>();
        }

        @Override
        public void close() throws Exception {

        }
    }
}
