package datawave.annotation.data.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * JUnit 5 unit tests for {@link FederatedAnnotationReader}.
 * <p>
 * Tests verify that the federated reader correctly aggregates results from multiple data access objects and handles exceptions gracefully.
 * </p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FederatedAnnotationReader Tests")
public class FederatedAnnotationReaderTest {

    private FederatedAnnotationReader federatedReader;
    private Map<String,AnnotationReader> mockDataAccesses;
    private AnnotationReader mockDao1;
    private AnnotationReader mockDao2;
    private AnnotationReader mockDao3;

    @BeforeEach
    void setUp() {
        mockDataAccesses = new HashMap<>();
        mockDao1 = mock(AnnotationReader.class);
        mockDao2 = mock(AnnotationReader.class);
        mockDao3 = mock(AnnotationReader.class);

        mockDataAccesses.put("dao1", mockDao1);
        mockDataAccesses.put("dao2", mockDao2);
        mockDataAccesses.put("dao3", mockDao3);

        federatedReader = new FederatedAnnotationReader(mockDataAccesses);
    }

    /**
     * Tests for the static getBest method
     */
    @Nested
    @DisplayName("GetBest static method tests")
    class GetBestTests {

        @Test
        @DisplayName("Should return empty Optional when given empty list")
        void testGetBestEmptyList() {
            Stream<String> results = Stream.empty();
            Optional<String> best = FederatedAnnotationReader.getBestDaoResponse(results);

            assertTrue(best.isEmpty());
        }

        @Test
        @DisplayName("Should return the only element when list has one item")
        void testGetBestSingleElement() {
            Stream<String> results = Stream.of("single-value");
            Optional<String> best = FederatedAnnotationReader.getBestDaoResponse(results);
            assertTrue(best.isPresent());
            assertEquals("single-value", best.get());
        }

        @Test
        @DisplayName("Should return first element when multiple elements are equal")
        void testGetBestMultipleEqualElements() {
            Stream<String> results = Stream.of("first", "first", "first");
            Optional<String> best = FederatedAnnotationReader.getBestDaoResponse(results);

            assertTrue(best.isPresent());
            assertEquals("first", best.get());
        }

        @Test
        @DisplayName("Should throw when multiple elements are not equal")
        void testGetBestMultipleNonEqualElementsThrows() {
            Stream<String> results = Stream.of("first", "second");
            RuntimeException exception = assertThrows(RuntimeException.class, () -> FederatedAnnotationReader.getBestDaoResponse(results));

            assertEquals("Conflicting federated results returned from multiple data sources", exception.getMessage());
        }
    }

    /**
     * Tests for getAnnotationSource method
     */
    @Nested
    @DisplayName("getAnnotationSource method tests")
    class GetAnnotationSourceTests {

        @Test
        @DisplayName("Should return empty Optional when no DAOs have the annotation source")
        void testGetAnnotationSourceNotFound() {
            String analyticHash = "hash-123";

            when(mockDao1.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());
            when(mockDao2.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());

            Optional<AnnotationSource> result = federatedReader.getAnnotationSource(analyticHash);

            assertTrue(result.isEmpty());
            verify(mockDao1, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao2, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao3, times(1)).getAnnotationSource(analyticHash);
        }

        @Test
        @DisplayName("Should return annotation source from first DAO that has it")
        void testGetAnnotationSourceFoundInFirstDao() {
            String analyticHash = "hash-456";
            AnnotationSource source = createTestAnnotationSource("engine1", "model1");

            when(mockDao1.getAnnotationSource(analyticHash)).thenReturn(Optional.of(source));
            when(mockDao2.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());

            Optional<AnnotationSource> result = federatedReader.getAnnotationSource(analyticHash);

            assertTrue(result.isPresent());
            assertEquals("engine1", result.get().getEngine());
            verify(mockDao1, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao2, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao3, times(1)).getAnnotationSource(analyticHash);
        }

        @Test
        @DisplayName("Should continue searching subsequent DAOs when first DAO throws exception")
        void testGetAnnotationSourceFirstDaoThrowsException() {
            String analyticHash = "hash-789";
            AnnotationSource source = createTestAnnotationSource("engine2", "model2");

            when(mockDao1.getAnnotationSource(analyticHash)).thenThrow(new RuntimeException("DAO1 Error"));
            when(mockDao2.getAnnotationSource(analyticHash)).thenReturn(Optional.of(source));
            when(mockDao3.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());

            Optional<AnnotationSource> result = federatedReader.getAnnotationSource(analyticHash);

            assertTrue(result.isPresent());
            assertEquals("engine2", result.get().getEngine());
            verify(mockDao1, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao2, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao3, times(1)).getAnnotationSource(analyticHash);
        }

        @Test
        @DisplayName("Should continue searching when multiple DAOs throw exceptions")
        void testGetAnnotationSourceMultipleDaosThrowExceptions() {
            String analyticHash = "hash-multiexc";
            AnnotationSource source = createTestAnnotationSource("engine3", "model3");

            when(mockDao1.getAnnotationSource(analyticHash)).thenThrow(new RuntimeException("DAO1 Error"));
            when(mockDao2.getAnnotationSource(analyticHash)).thenThrow(new RuntimeException("DAO2 Error"));
            when(mockDao3.getAnnotationSource(analyticHash)).thenReturn(Optional.of(source));

            Optional<AnnotationSource> result = federatedReader.getAnnotationSource(analyticHash);

            assertTrue(result.isPresent());
            assertEquals("engine3", result.get().getEngine());
            verify(mockDao1, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao2, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao3, times(1)).getAnnotationSource(analyticHash);
        }

        @Test
        @DisplayName("Should return empty Optional when all DAOs throw exceptions")
        void testGetAnnotationSourceAllDaosThrowExceptions() {
            String analyticHash = "hash-allexc";

            when(mockDao1.getAnnotationSource(analyticHash)).thenThrow(new RuntimeException("DAO1 Error"));
            when(mockDao2.getAnnotationSource(analyticHash)).thenThrow(new RuntimeException("DAO2 Error"));
            when(mockDao3.getAnnotationSource(analyticHash)).thenThrow(new RuntimeException("DAO3 Error"));

            Optional<AnnotationSource> result = federatedReader.getAnnotationSource(analyticHash);

            assertTrue(result.isEmpty());
            verify(mockDao1, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao2, times(1)).getAnnotationSource(analyticHash);
            verify(mockDao3, times(1)).getAnnotationSource(analyticHash);
        }

        @Test
        @DisplayName("Should handle null analyticHash parameter")
        void testGetAnnotationSourceNullHash() {
            when(mockDao1.getAnnotationSource(null)).thenReturn(Optional.empty());
            when(mockDao2.getAnnotationSource(null)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotationSource(null)).thenReturn(Optional.empty());

            Optional<AnnotationSource> result = federatedReader.getAnnotationSource(null);

            assertTrue(result.isEmpty());
        }
    }

    /**
     * Tests for getAnnotation(shard, datatype, uid, annotationType, annotationUid) method
     */
    @Nested
    @DisplayName("getAnnotation (5-parameter) method tests")
    class GetAnnotation5ParameterTests {

        @Test
        @DisplayName("Should return empty Optional when no DAOs have the annotation")
        void testGetAnnotationNotFound() {
            String shard = "shard-001";
            String datatype = "news";
            String uid = "uid-123";
            String annotationType = "tokens";
            String annotationUid = "anno-456";

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationType, annotationUid);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return annotation from first DAO that has it")
        void testGetAnnotationFoundInFirstDao() {
            String shard = "shard-002";
            String datatype = "news";
            String uid = "uid-789";
            String annotationType = "tokens";
            String annotationUid = "anno-999";
            Annotation annotation = createTestAnnotation(shard, datatype, uid, annotationType);

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.of(annotation));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationType, annotationUid);

            assertTrue(result.isPresent());
            assertEquals(annotationType, result.get().getAnnotationType());
        }

        @Test
        @DisplayName("Should return a single copy when multiple DAOs return same annotation")
        void testGetAnnotationDuplicateAcrossDaos() {
            String shard = "shard-002";
            String datatype = "news";
            String uid = "uid-789";
            String annotationType = "tokens";
            String annotationUid = "anno-999";
            Annotation annotation = createTestAnnotation(shard, datatype, uid, annotationType);

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.of(annotation));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.of(annotation));
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationType, annotationUid);

            assertTrue(result.isPresent());
            assertEquals(annotation, result.get());
            verify(mockDao1, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
            verify(mockDao2, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
            verify(mockDao3, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
        }

        @Test
        @DisplayName("Should throw when multiple DAOs return conflicting annotations")
        void testGetAnnotationConflictingAcrossDaosThrows() {
            String shard = "shard-002";
            String datatype = "news";
            String uid = "uid-789";
            String annotationType = "tokens";
            String annotationUid = "anno-999";
            Annotation annotationOne = createTestAnnotation(shard, datatype, uid, annotationType).toBuilder().setAnnotationId("id-one").build();
            Annotation annotationTwo = createTestAnnotation(shard, datatype, uid, annotationType).toBuilder().setAnnotationId("id-two").build();

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.of(annotationOne));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.of(annotationTwo));
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());

            RuntimeException exception = assertThrows(RuntimeException.class,
                            () -> federatedReader.getAnnotation(shard, datatype, uid, annotationType, annotationUid));

            assertEquals("Conflicting federated results returned from multiple data sources", exception.getMessage());
            verify(mockDao1, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
            verify(mockDao2, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
            verify(mockDao3, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
        }

        @Test
        @DisplayName("Should skip DAO that throws exception and try next")
        void testGetAnnotationSkipExceptionDao() {
            String shard = "shard-003";
            String datatype = "email";
            String uid = "uid-222";
            String annotationType = "sentiment";
            String annotationUid = "anno-333";
            Annotation annotation = createTestAnnotation(shard, datatype, uid, annotationType);

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenThrow(new RuntimeException("Connection failed"));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.of(annotation));
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationType, annotationUid)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationType, annotationUid);

            assertTrue(result.isPresent());
            verify(mockDao1, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
            verify(mockDao2, times(1)).getAnnotation(shard, datatype, uid, annotationType, annotationUid);
        }
    }

    /**
     * Tests for getAnnotation(shard, datatype, uid, annotationId) method
     */
    @Nested
    @DisplayName("getAnnotation (4-parameter) method tests")
    class GetAnnotation4ParameterTests {

        @Test
        @DisplayName("Should return empty Optional when no DAOs have the annotation")
        void testGetAnnotationByIdNotFound() {
            String shard = "shard-004";
            String datatype = "social";
            String uid = "uid-444";
            String annotationId = "anno-id-555";

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationId);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return annotation from first DAO that has it")
        void testGetAnnotationByIdFoundInFirstDao() {
            String shard = "shard-005";
            String datatype = "blog";
            String uid = "uid-555";
            String annotationId = "anno-id-666";
            Annotation annotation = createTestAnnotation(shard, datatype, uid, "type1");

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.of(annotation));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationId);

            assertTrue(result.isPresent());
            assertEquals(shard, result.get().getShard());
        }

        @Test
        @DisplayName("Should return a single copy when multiple DAOs return same annotation by id")
        void testGetAnnotationByIdDuplicateAcrossDaos() {
            String shard = "shard-005";
            String datatype = "blog";
            String uid = "uid-555";
            String annotationId = "anno-id-666";
            Annotation annotation = createTestAnnotation(shard, datatype, uid, "type1");

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.of(annotation));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.of(annotation));
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationId);

            assertTrue(result.isPresent());
            assertEquals(annotation, result.get());
            verify(mockDao1, times(1)).getAnnotation(shard, datatype, uid, annotationId);
            verify(mockDao2, times(1)).getAnnotation(shard, datatype, uid, annotationId);
            verify(mockDao3, times(1)).getAnnotation(shard, datatype, uid, annotationId);
        }

        @Test
        @DisplayName("Should throw when multiple DAOs return conflicting annotations by id")
        void testGetAnnotationByIdConflictingAcrossDaosThrows() {
            String shard = "shard-005";
            String datatype = "blog";
            String uid = "uid-555";
            String annotationId = "anno-id-666";
            Annotation annotationOne = createTestAnnotation(shard, datatype, uid, "type1").toBuilder().setAnnotationId("id-one").build();
            Annotation annotationTwo = createTestAnnotation(shard, datatype, uid, "type1").toBuilder().setAnnotationId("id-two").build();

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.of(annotationOne));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.of(annotationTwo));
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());

            RuntimeException exception = assertThrows(RuntimeException.class, () -> federatedReader.getAnnotation(shard, datatype, uid, annotationId));

            assertEquals("Conflicting federated results returned from multiple data sources", exception.getMessage());
            verify(mockDao1, times(1)).getAnnotation(shard, datatype, uid, annotationId);
            verify(mockDao2, times(1)).getAnnotation(shard, datatype, uid, annotationId);
            verify(mockDao3, times(1)).getAnnotation(shard, datatype, uid, annotationId);
        }

        @Test
        @DisplayName("Should continue searching when first DAO throws exception")
        void testGetAnnotationByIdFirstDaoThrowsException() {
            String shard = "shard-006";
            String datatype = "comment";
            String uid = "uid-666";
            String annotationId = "anno-id-777";
            Annotation annotation = createTestAnnotation(shard, datatype, uid, "type2");

            when(mockDao1.getAnnotation(shard, datatype, uid, annotationId)).thenThrow(new RuntimeException("DB Error"));
            when(mockDao2.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.of(annotation));
            when(mockDao3.getAnnotation(shard, datatype, uid, annotationId)).thenReturn(Optional.empty());

            Optional<Annotation> result = federatedReader.getAnnotation(shard, datatype, uid, annotationId);

            assertTrue(result.isPresent());
            verify(mockDao1, times(1)).getAnnotation(shard, datatype, uid, annotationId);
            verify(mockDao2, times(1)).getAnnotation(shard, datatype, uid, annotationId);
        }
    }

    /**
     * Tests for getAnnotationTypes method
     */
    @Nested
    @DisplayName("getAnnotationTypes method tests")
    class GetAnnotationTypesTests {

        @Test
        @DisplayName("Should return empty collection when no DAOs have annotation types")
        void testGetAnnotationTypesEmpty() {
            String shard = "shard-007";
            String datatype = "news";
            String uid = "uid-777";

            when(mockDao1.getAnnotationTypes(shard, datatype, uid)).thenReturn(new ArrayList<>());
            when(mockDao2.getAnnotationTypes(shard, datatype, uid)).thenReturn(new ArrayList<>());
            when(mockDao3.getAnnotationTypes(shard, datatype, uid)).thenReturn(new ArrayList<>());

            Collection<String> result = federatedReader.getAnnotationTypes(shard, datatype, uid);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should combine annotation types from all DAOs")
        void testGetAnnotationTypesCombined() {
            String shard = "shard-008";
            String datatype = "email";
            String uid = "uid-888";

            List<String> dao1Types = List.of("sentiment", "entities");
            List<String> dao2Types = List.of("tokens", "tags");
            List<String> dao3Types = List.of("entities", "summary"); // entities appears twice

            when(mockDao1.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao1Types);
            when(mockDao2.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao2Types);
            when(mockDao3.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao3Types);

            Collection<String> result = federatedReader.getAnnotationTypes(shard, datatype, uid);

            // Result should be a Set, so no duplicates
            assertEquals(5, result.size());
            assertTrue(result.contains("sentiment"));
            assertTrue(result.contains("entities"));
            assertTrue(result.contains("tokens"));
            assertTrue(result.contains("tags"));
            assertTrue(result.contains("summary"));
        }

        @Test
        @DisplayName("Should skip DAO that throws exception")
        void testGetAnnotationTypesSkipExceptionDao() {
            String shard = "shard-009";
            String datatype = "chat";
            String uid = "uid-999";

            List<String> dao2Types = List.of("language", "intent");
            List<String> dao3Types = List.of("emotion");

            when(mockDao1.getAnnotationTypes(shard, datatype, uid)).thenThrow(new RuntimeException("DAO Error"));
            when(mockDao2.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao2Types);
            when(mockDao3.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao3Types);

            Collection<String> result = federatedReader.getAnnotationTypes(shard, datatype, uid);

            assertEquals(3, result.size());
            assertTrue(result.contains("language"));
            assertTrue(result.contains("intent"));
            assertTrue(result.contains("emotion"));
            verify(mockDao1, times(1)).getAnnotationTypes(shard, datatype, uid);
        }

        @Test
        @DisplayName("Should return TreeSet for sorted results")
        void testGetAnnotationTypesReturnsSortedSet() {
            String shard = "shard-010";
            String datatype = "document";
            String uid = "uid-000";

            List<String> dao1Types = List.of("zebra", "apple", "monkey");
            List<String> dao2Types = List.of("banana");

            when(mockDao1.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao1Types);
            when(mockDao2.getAnnotationTypes(shard, datatype, uid)).thenReturn(dao2Types);
            when(mockDao3.getAnnotationTypes(shard, datatype, uid)).thenReturn(new ArrayList<>());

            Collection<String> result = federatedReader.getAnnotationTypes(shard, datatype, uid);

            assertEquals(4, result.size());
            // Verify order - TreeSet maintains sorted order
            List<String> resultList = new ArrayList<>(result);
            assertEquals("apple", resultList.get(0));
            assertEquals("banana", resultList.get(1));
            assertEquals("monkey", resultList.get(2));
            assertEquals("zebra", resultList.get(3));
        }
    }

    /**
     * Tests for getAnnotations method
     */
    @Nested
    @DisplayName("getAnnotations method tests")
    class GetAnnotationsTests {

        @Test
        @DisplayName("Should return empty list when no DAOs have annotations")
        void testGetAnnotationsEmpty() {
            String shard = "shard-011";
            String datatype = "news";
            String uid = "uid-111";

            when(mockDao1.getAnnotations(shard, datatype, uid)).thenReturn(new ArrayList<>());
            when(mockDao2.getAnnotations(shard, datatype, uid)).thenReturn(new ArrayList<>());
            when(mockDao3.getAnnotations(shard, datatype, uid)).thenReturn(new ArrayList<>());

            Collection<Annotation> result = federatedReader.getAnnotations(shard, datatype, uid);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should combine annotations from all DAOs")
        void testGetAnnotationsCombined() {
            String shard = "shard-012";
            String datatype = "social";
            String uid = "uid-222";

            Annotation anno1 = createTestAnnotation(shard, datatype, uid, "type1");
            Annotation anno2 = createTestAnnotation(shard, datatype, uid, "type2");
            Annotation anno3 = createTestAnnotation(shard, datatype, uid, "type3");
            Annotation anno4 = createTestAnnotation(shard, datatype, uid, "type4");

            when(mockDao1.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno1));
            when(mockDao2.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno2, anno3));
            when(mockDao3.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno4));

            Collection<Annotation> result = federatedReader.getAnnotations(shard, datatype, uid);

            assertEquals(4, result.size());
            assertTrue(result.contains(anno1));
            assertTrue(result.contains(anno2));
            assertTrue(result.contains(anno3));
            assertTrue(result.contains(anno4));
        }

        @Test
        @DisplayName("Should deduplicate equal annotations from multiple DAOs")
        void testGetAnnotationsDuplicates() {
            String shard = "shard-013";
            String datatype = "email";
            String uid = "uid-333";

            Annotation anno1 = createTestAnnotation(shard, datatype, uid, "sentiment");
            Annotation anno1Dup = anno1;

            when(mockDao1.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno1));
            when(mockDao2.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno1Dup));
            when(mockDao3.getAnnotations(shard, datatype, uid)).thenReturn(new ArrayList<>());

            Collection<Annotation> result = federatedReader.getAnnotations(shard, datatype, uid);

            assertEquals(1, result.size());
            assertEquals(anno1, result.stream().findFirst().orElse(null));
        }

        @Test
        @DisplayName("Should skip DAO that throws exception and continue")
        void testGetAnnotationsSkipExceptionDao() {
            String shard = "shard-014";
            String datatype = "chat";
            String uid = "uid-444";

            Annotation anno1 = createTestAnnotation(shard, datatype, uid, "type1");
            Annotation anno2 = createTestAnnotation(shard, datatype, uid, "type2");

            when(mockDao1.getAnnotations(shard, datatype, uid)).thenThrow(new RuntimeException("Connection error"));
            when(mockDao2.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno1));
            when(mockDao3.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno2));

            Collection<Annotation> result = federatedReader.getAnnotations(shard, datatype, uid);

            assertEquals(2, result.size());
            verify(mockDao1, times(1)).getAnnotations(shard, datatype, uid);
            verify(mockDao2, times(1)).getAnnotations(shard, datatype, uid);
            verify(mockDao3, times(1)).getAnnotations(shard, datatype, uid);
        }

        @Test
        @DisplayName("Should handle all DAOs throwing exceptions")
        void testGetAnnotationsAllDaosThrowExceptions() {
            String shard = "shard-015";
            String datatype = "document";
            String uid = "uid-555";

            when(mockDao1.getAnnotations(shard, datatype, uid)).thenThrow(new RuntimeException("Error 1"));
            when(mockDao2.getAnnotations(shard, datatype, uid)).thenThrow(new RuntimeException("Error 2"));
            when(mockDao3.getAnnotations(shard, datatype, uid)).thenThrow(new RuntimeException("Error 3"));

            Collection<Annotation> result = federatedReader.getAnnotations(shard, datatype, uid);

            assertTrue(result.isEmpty());
        }
    }

    /**
     * Tests for getAnnotationsForType method
     */
    @Nested
    @DisplayName("getAnnotationsForType method tests")
    class GetAnnotationsForTypeTests {

        @Test
        @DisplayName("Should return empty list when no DAOs have annotations for type")
        void testGetAnnotationsForTypeEmpty() {
            String shard = "shard-016";
            String datatype = "news";
            String uid = "uid-666";
            String annotationType = "tokens";

            when(mockDao1.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(new ArrayList<>());
            when(mockDao2.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(new ArrayList<>());
            when(mockDao3.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(new ArrayList<>());

            Collection<Annotation> result = federatedReader.getAnnotationsForType(shard, datatype, uid, annotationType);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should combine annotations from all DAOs regardless of type parameter")
        void testGetAnnotationsForTypeCombined() {
            String shard = "shard-017";
            String datatype = "social";
            String uid = "uid-777";
            String annotationType = "sentiment";

            Annotation anno1 = createTestAnnotation(shard, datatype, uid, "type1");
            Annotation anno2 = createTestAnnotation(shard, datatype, uid, "type2");
            Annotation anno3 = createTestAnnotation(shard, datatype, uid, "type3");

            // Note: The implementation currently ignores the annotationType parameter
            when(mockDao1.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(List.of(anno1));
            when(mockDao2.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(List.of(anno2));
            when(mockDao3.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(List.of(anno3));

            Collection<Annotation> result = federatedReader.getAnnotationsForType(shard, datatype, uid, annotationType);

            assertEquals(3, result.size());
        }

        @Test
        @DisplayName("Should deduplicate equal annotations for type from multiple DAOs")
        void testGetAnnotationsForTypeDuplicates() {
            String shard = "shard-017";
            String datatype = "social";
            String uid = "uid-777";
            String annotationType = "sentiment";

            Annotation anno1 = createTestAnnotation(shard, datatype, uid, "type1");

            when(mockDao1.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(List.of(anno1));
            when(mockDao2.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(List.of(anno1));
            when(mockDao3.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(new ArrayList<>());

            Collection<Annotation> result = federatedReader.getAnnotationsForType(shard, datatype, uid, annotationType);

            assertEquals(1, result.size());
            assertEquals(anno1, result.stream().findFirst().orElse(null));
        }

        @Test
        @DisplayName("Should skip DAO that throws exception")
        void testGetAnnotationsForTypeSkipExceptionDao() {
            String shard = "shard-018";
            String datatype = "email";
            String uid = "uid-888";
            String annotationType = "entities";

            Annotation anno1 = createTestAnnotation(shard, datatype, uid, "type1");
            Annotation anno2 = createTestAnnotation(shard, datatype, uid, "type2");

            when(mockDao1.getAnnotationsForType(shard, datatype, uid, annotationType)).thenThrow(new RuntimeException("DAO Error"));
            when(mockDao2.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(List.of(anno1, anno2));
            when(mockDao3.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(new ArrayList<>());

            Collection<Annotation> result = federatedReader.getAnnotationsForType(shard, datatype, uid, annotationType);

            assertEquals(2, result.size());
            verify(mockDao1, times(1)).getAnnotationsForType(shard, datatype, uid, annotationType);
        }

        @Test
        @DisplayName("Should handle multiple DAOs returning annotations")
        void testGetAnnotationsForTypeMultipleDaos() {
            String shard = "shard-019";
            String datatype = "chat";
            String uid = "uid-999";
            String annotationType = "language";

            List<Annotation> daosAnnotations = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                daosAnnotations.add(createTestAnnotation(shard, datatype, uid, "type" + i));
            }

            when(mockDao1.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(daosAnnotations.subList(0, 2));
            when(mockDao2.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(daosAnnotations.subList(2, 4));
            when(mockDao3.getAnnotationsForType(shard, datatype, uid, annotationType)).thenReturn(daosAnnotations.subList(4, 5));

            Collection<Annotation> result = federatedReader.getAnnotationsForType(shard, datatype, uid, annotationType);

            assertEquals(5, result.size());
        }
    }

    /**
     * Tests for constructor and initialization
     */
    @Nested
    @DisplayName("Constructor and initialization tests")
    class ConstructorTests {

        @Test
        @DisplayName("Should initialize with empty DAOs map")
        void testConstructorEmptyMap() {
            FederatedAnnotationReader reader = new FederatedAnnotationReader(new HashMap<>());

            assertNotNull(reader);
            Optional<String> result = FederatedAnnotationReader.getBestDaoResponse(Stream.<String> empty());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should initialize with single DAO")
        void testConstructorSingleDao() {
            Map<String,AnnotationReader> singleDaoMap = new HashMap<>();
            AnnotationReader dao = mock(AnnotationReader.class);
            singleDaoMap.put("single-dao", dao);

            FederatedAnnotationReader reader = new FederatedAnnotationReader(singleDaoMap);

            String hash = "test-hash";
            when(dao.getAnnotationSource(hash)).thenReturn(Optional.empty());

            Optional<AnnotationSource> result = reader.getAnnotationSource(hash);

            assertTrue(result.isEmpty());
            verify(dao, times(1)).getAnnotationSource(hash);
        }

        @Test
        @DisplayName("Should initialize with multiple DAOs")
        void testConstructorMultipleDaos() {
            assertNotNull(federatedReader);
            assertEquals(3, mockDataAccesses.size());
            assertTrue(mockDataAccesses.containsKey("dao1"));
            assertTrue(mockDataAccesses.containsKey("dao2"));
            assertTrue(mockDataAccesses.containsKey("dao3"));
        }
    }

    @Nested
    @DisplayName("Async timeout behavior tests")
    class AsyncTimeoutTests {

        @Test
        @DisplayName("Should skip slow DAO results that exceed timeout")
        void testSlowDaoTimesOut() {
            ExecutorService executor = Executors.newFixedThreadPool(3);
            try {
                Map<String,AnnotationReader> localDaos = new HashMap<>();
                AnnotationReader slowDao = mock(AnnotationReader.class);
                AnnotationReader fastDao = mock(AnnotationReader.class);

                String analyticHash = "hash-timeout";
                AnnotationSource fastSource = createTestAnnotationSource("fast-engine", "fast-model");

                when(slowDao.getAnnotationSource(analyticHash)).thenAnswer(invocation -> {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return Optional.of(createTestAnnotationSource("slow-engine", "slow-model"));
                });
                when(fastDao.getAnnotationSource(analyticHash)).thenReturn(Optional.of(fastSource));

                localDaos.put("slow", slowDao);
                localDaos.put("fast", fastDao);

                FederatedAnnotationReader timeoutReader = new FederatedAnnotationReader(localDaos, executor, 25);
                Optional<AnnotationSource> result = timeoutReader.getAnnotationSource(analyticHash);

                assertTrue(result.isPresent());
                assertEquals(fastSource, result.get());
            } finally {
                executor.shutdownNow();
                try {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Integration-style tests for multiple method calls
     */
    @Nested
    @DisplayName("Integration tests for multiple method interactions")
    class IntegrationTests {

        @Test
        @DisplayName("Should aggregate results from multiple query types")
        void testMultipleQueryTypes() {
            String shard = "shard-020";
            String datatype = "news";
            String uid = "uid-aaa";
            String analyticHash = "hash-bbb";

            AnnotationSource source = createTestAnnotationSource("engine", "model");
            Annotation anno = createTestAnnotation(shard, datatype, uid, "tokens");

            when(mockDao1.getAnnotationSource(analyticHash)).thenReturn(Optional.of(source));
            when(mockDao1.getAnnotation(shard, datatype, uid, "tokens", "anno-id")).thenReturn(Optional.of(anno));
            when(mockDao1.getAnnotationTypes(shard, datatype, uid)).thenReturn(List.of("tokens", "entities"));
            when(mockDao1.getAnnotations(shard, datatype, uid)).thenReturn(List.of(anno));

            when(mockDao2.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());
            when(mockDao2.getAnnotation(shard, datatype, uid, "tokens", "anno-id")).thenReturn(Optional.empty());
            when(mockDao2.getAnnotationTypes(shard, datatype, uid)).thenReturn(List.of("sentiment"));
            when(mockDao2.getAnnotations(shard, datatype, uid)).thenReturn(new ArrayList<>());

            when(mockDao3.getAnnotationSource(analyticHash)).thenReturn(Optional.empty());
            when(mockDao3.getAnnotation(shard, datatype, uid, "tokens", "anno-id")).thenReturn(Optional.empty());
            when(mockDao3.getAnnotationTypes(shard, datatype, uid)).thenReturn(new ArrayList<>());
            when(mockDao3.getAnnotations(shard, datatype, uid)).thenReturn(new ArrayList<>());

            // Execute queries
            Optional<AnnotationSource> sourceResult = federatedReader.getAnnotationSource(analyticHash);
            Optional<Annotation> annotationResult = federatedReader.getAnnotation(shard, datatype, uid, "tokens", "anno-id");
            Collection<String> typesResult = federatedReader.getAnnotationTypes(shard, datatype, uid);
            Collection<Annotation> annotationsResult = federatedReader.getAnnotations(shard, datatype, uid);

            // Verify results
            assertTrue(sourceResult.isPresent());
            assertEquals("engine", sourceResult.get().getEngine());

            assertTrue(annotationResult.isPresent());
            assertEquals("tokens", annotationResult.get().getAnnotationType());

            assertEquals(3, typesResult.size());
            assertTrue(typesResult.contains("tokens"));
            assertTrue(typesResult.contains("entities"));
            assertTrue(typesResult.contains("sentiment"));

            assertEquals(1, annotationsResult.size());
            assertEquals(anno, annotationsResult.stream().findFirst().orElse(null));
        }
    }

    /**
     * Helper method to create test AnnotationSource
     */
    private AnnotationSource createTestAnnotationSource(String engine, String model) {
        return AnnotationSource.newBuilder().setEngine(engine).setModel(model).setPlatform("test-platform").setAnalyticSourceHash("hash-" + System.nanoTime())
                        .putMetadata("visibility", "PUBLIC").build();
    }

    /**
     * Helper method to create test Annotation
     */
    private Annotation createTestAnnotation(String shard, String datatype, String uid, String annotationType) {
        return Annotation.newBuilder().setShard(shard).setDataType(datatype).setUid(uid).setAnnotationType(annotationType)
                        .setAnnotationId("id-" + System.nanoTime()).putMetadata("visibility", "PUBLIC").build();
    }
}
