package datawave.microservice.annotation.service;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationSourcesEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertSegmentsEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateMultiTestSegment;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotationSource;
import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;

import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.helpers.PrintUtility;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import datawave.microservice.annotation.util.Metadata;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.query.QueryTestTableHelper;
import datawave.query.util.WiseGuysIngest;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.table.constants.TableName;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({"unchecked", "SpellCheckingInspection"})
@ExtendWith(MockitoExtension.class)
@Slf4j
public class TestAnnotationControllerV1 {

    private static final TimestampTransformer timestampTransformer = new DefaultTimestampTransformer();
    private static final VisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
    private static final AnnotationProperties annotationProperties = new AnnotationProperties();

    private static final MultiValueMap<String,String> EMPTY_HTTP_HEADERS = new HttpHeaders();

    private static AccumuloClient client;

    @Mock
    private AccumuloConnectionFactory connectionFactory;

    @Mock
    private LookupService lookupService;

    private DatawaveUserDetails defaultUserDetails;

    @Mock
    private AnnotationSupplier annotationSink;

    private AnnotationControllerV1 annotationController;

    @BeforeAll
    public static void setupAccumulo() throws Exception {
        org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TestAnnotationControllerV1.class);
        QueryTestTableHelper queryTestTableHelper = new QueryTestTableHelper(TestAnnotationControllerV1.class.toString(), log);
        client = queryTestTableHelper.client;

        String annotationTableName = "annotation";
        String annotationSourceTableName = "annotationSource";

        TableOperations tops = client.tableOperations();
        tops.create(annotationTableName);
        tops.create(annotationSourceTableName);

        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
        AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer(visibilityTransformer, timestampTransformer);

        Authorizations auths = new Authorizations("ALL", "PUBLIC");
        AnnotationDataAccess testDao = new AnnotationDataAccess(client, Set.of(auths), annotationTableName, annotationSourceTableName, annotationSerializer,
                        annotationSourceSerializer);

        Annotation testAnnotation = generateTestAnnotation();
        testDao.addAnnotation(testAnnotation);

        AnnotationSource testAnnotationSource = generateTestAnnotationSource();
        testDao.addAnnotationSource(testAnnotationSource);

        // Configurator.setLevel(PrintUtility.class, Level.DEBUG);

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);

        testDao.addAnnotation(generateCorleoneAnnotation());

        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        PrintUtility.printTable(client, auths, annotationTableName);
        PrintUtility.printTable(client, auths, annotationSourceTableName);

        annotationProperties.setAnnotationTableName(annotationTableName);
        annotationProperties.setAnnotationSourceTableName(annotationSourceTableName);
        annotationProperties.setEnableInternalIdLookup(true);
        annotationProperties.setSystemFrom("annotation");
    }

    @SuppressWarnings("SpellCheckingInspection")
    public static Annotation generateCorleoneAnnotation() {
        AnnotationSource baseAnnotationSource = generateTestAnnotationSource();
        AnnotationSource annotationSource = AnnotationUtils.injectAnnotationSourceHashes(baseAnnotationSource);

        Map<String,String> metadata = new HashMap<>();
        metadata.put("UUID", "CORLEONE");
        metadata.put("visibility", "ALL");
        metadata.put("created_date", "2025-10-01T00:00:00.000Z");

        //@formatter:off
        return Annotation.newBuilder()
                .setShard("20130101_0")
                .setDataType("test")
                .setUid("-d5uxna.msizfm.-oxy0iu")
                .setAnnotationType("corleoneAnnotationType")
                .setDocumentId("CORLEONE")
                .setSource(annotationSource)
                .addAllSegments(List.of(generateMultiTestSegment()))
                .putAllMetadata(metadata)
                .build();
        //@formatter:on
    }

    @BeforeEach
    void setUp() throws Exception {
        SubjectIssuerDNPair DEFAULT_USER_DN = SubjectIssuerDNPair.of("userDn", "issuerDn");
        Collection<String> DEFAULT_ROLES = Collections.singleton("Administrator");
        Collection<String> DEFAULT_AUTHS = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "ALL", "PUBLIC");
        DatawaveUser dwUser = new DatawaveUser(DEFAULT_USER_DN, USER, DEFAULT_AUTHS, DEFAULT_ROLES, null, System.currentTimeMillis());
        defaultUserDetails = new DatawaveUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime());

        annotationController = new AnnotationControllerV1(connectionFactory, lookupService, annotationProperties, timestampTransformer, visibilityTransformer,
                        annotationSink);
        lenient().when(connectionFactory.getClient(any(), any(), any(), any(), any())).thenReturn(client);
    }

    /**
     * Simulates a message broker that immediately (synchronously) acknowledges every message sent through {@code annotationSink}, so that
     * {@code writeAnnotation}/{@code updateAnnotation} calls that go through the durable-write acknowledgement protocol don't have to wait out a real ack
     * timeout in these tests.
     */
    private void configureImmediateAck() {
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            org.springframework.messaging.Message<?> sent = invocation.getArgument(0);
            Object correlationId = sent.getHeaders().get(org.springframework.integration.IntegrationMessageHeaderAccessor.CORRELATION_ID);
            assertNotNull(correlationId);
            org.springframework.messaging.Message<String> ack = org.springframework.integration.support.MessageBuilder.withPayload("ack")
                            .setCorrelationId(correlationId).build();
            annotationController.processConfirmAck(ack);
            return true;
        });
    }

    @Test
    public void testGetAnnotationSource() {
        AnnotationSource baseAnnotationSource = generateTestAnnotationSource();
        AnnotationSource expectedAnnotationSource = AnnotationUtils.injectAnnotationSourceHashes(baseAnnotationSource);

        ResponseEntity<?> response = annotationController.getAnnotationSource(expectedAnnotationSource.getAnalyticSourceHash(), EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(200, response);
        AnnotationSource annotationSource = assertExpectedEntity(AnnotationSource.class, response);
        assertNotNull(annotationSource);
        assertAnnotationSourcesEqual(expectedAnnotationSource, annotationSource);
    }

    @Test
    public void testGetMissingAnnotationSource() {
        ResponseEntity<?> response = annotationController.getAnnotationSource("52EF0E07742AC65873C6DF80759AF193", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotation source found for analyticHash", errorResponse);
        assertContains("52EF0E07742AC65873C6DF80759AF193", errorResponse);
    }

    @Test
    public void testGetAnnotationTypesInternalId() {
        Metadata expectedMetadata = new Metadata("shard", "20250704_249", "testDataType", "abcde.fghij.klmno");
        ResponseEntity<?> response = annotationController.getAnnotationTypes("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(200, response);
        Map<Metadata,Collection<String>> annotationTypeMap = assertExpectedEntity(HashMap.class, response);
        assertEquals(1, annotationTypeMap.size());
        Collection<String> annotationTypeList = annotationTypeMap.get(expectedMetadata);
        assertNotNull(annotationTypeList);
        assertEquals(1, annotationTypeList.size());
        assertTrue(annotationTypeList.contains("testAnnotationType"));
    }

    @Test
    public void testGetAnnotationTypesMissingInternalId() {
        ResponseEntity<?> response = annotationController.getAnnotationTypes("DOCUMENT", "20250704_249/testDataType/12345.67890.12345", EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotation types found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/12345.67890.12345", errorResponse);
    }

    @Test
    public void testGetAnnotationTypesExternalIdNoAnnotations() {
        Metadata caponeMetadata = new Metadata("shard", "20130101_0", "test", "-cvy0gj.tlf59s.-duxzua");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CAPONE"), any(), any(), any())).thenReturn(List.of(caponeMetadata));

        ResponseEntity<?> response = annotationController.getAnnotationTypes("UUID", "CAPONE", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotation types found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
    }

    @Test
    public void testGetAnnotationTypesExternalIdWithAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-d5uxna.msizfm.-oxy0iu");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CORLEONE"), any(), any(), any())).thenReturn(List.of(expectedMetadata));
        ResponseEntity<?> response = annotationController.getAnnotationTypes("UUID", "CORLEONE", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(200, response);
        HashMap<Metadata,Collection<String>> annotationTypeMap = assertExpectedEntity(HashMap.class, response);
        assertEquals(1, annotationTypeMap.size());
        Collection<String> annotationTypeList = annotationTypeMap.get(expectedMetadata);
        assertNotNull(annotationTypeList);
        assertEquals(1, annotationTypeList.size());
        assertTrue(annotationTypeList.contains("corleoneAnnotationType"));
    }

    @Test
    public void testGetAnnotationsForInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        ResponseEntity<?> response = annotationController.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAnnotationsForMissingInternalId() {
        ResponseEntity<?> response = annotationController.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/12345.67890.12345", EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/12345.67890.12345", errorResponse);
    }

    @Test
    public void testGetAnnotationsForExternalIdNoAnnotations() {
        Metadata caponeMetadata = new Metadata("shard", "20130101_0", "test", "-cvy0gj.tlf59s.-duxzua");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CAPONE"), any(), any(), any())).thenReturn(List.of(caponeMetadata));

        ResponseEntity<?> response = annotationController.getAnnotationsFor("UUID", "CAPONE", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
    }

    @Test
    public void testAnnotationsForExternalIdWithAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-d5uxna.msizfm.-oxy0iu");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CORLEONE"), any(), any(), any())).thenReturn(List.of(expectedMetadata));

        Annotation testAnnotation = generateCorleoneAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        ResponseEntity<?> response = annotationController.getAnnotationsFor("UUID", "CORLEONE", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAllAnnotationsByTypeInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        // TODO: insert a second annotation for the same document with a different type?
        //@formatter:off
        ResponseEntity<?> response = annotationController.getAnnotationsByType(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "testAnnotationType",EMPTY_HTTP_HEADERS, defaultUserDetails
        );
        //@formatter:on
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAllAnnotationByTypeInternalIdMissingType() {
        // TODO: insert a second annotation for the same document with a different type?
        //@formatter:off
        ResponseEntity<?> response = annotationController.getAnnotationsByType(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "missingType", EMPTY_HTTP_HEADERS, defaultUserDetails
        );
        //@formatter:on
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations of type found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("missingType", errorResponse);
    }

    @Test
    public void testGetAllAnnotationByTypeExternalIdNoAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-cvy0gj.tlf59s.-duxzua");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CAPONE"), any(), any(), any())).thenReturn(List.of(expectedMetadata));

        ResponseEntity<?> response = annotationController.getAnnotationsByType("UUID", "CAPONE", "testAnnotationType", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations of type found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
        assertContains("UUID:CAPONE", errorResponse);
        assertContains("testAnnotationType", errorResponse);

    }

    @Test
    public void testGetAllAnnotationByTypeExternalIdWithAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-d5uxna.msizfm.-oxy0iu");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CORLEONE"), any(), any(), any())).thenReturn(List.of(expectedMetadata));

        Annotation testAnnotation = generateCorleoneAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        ResponseEntity<?> response = annotationController.getAnnotationsByType("UUID", "CORLEONE", "corleoneAnnotationType", EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAnnotationInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        ResponseEntity<?> response = annotationController.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "23BD91EC",
                        EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Test
    public void testGetAnnotationInternalIdDisabled() {
        annotationProperties.setEnableInternalIdLookup(false);
        try {
            ResponseEntity<?> response = annotationController.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "23BD91EC",
                            EMPTY_HTTP_HEADERS, defaultUserDetails);
            assertResponseStatus(500, response);
            String errorResponse = assertExpectedEntity(String.class, response);
            assertContains("Internal identifier lookup is disabled for", errorResponse);
            assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        } finally {
            // annotationProperties is shared across all tests in this class (initialized once in setupAccumulo), so restore
            // it to avoid affecting other tests that rely on internal id lookup being enabled.
            annotationProperties.setEnableInternalIdLookup(true);
        }
    }

    @Test
    public void testGetAnnotationMissingInternalId() {
        ResponseEntity<?> response = annotationController.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "aaaaaaaa",
                        EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("aaaaaaaa", errorResponse);
    }

    @Test
    public void testGetAnnotationExternalIdNoAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-cvy0gj.tlf59s.-duxzua");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CAPONE"), any(), any(), any())).thenReturn(List.of(expectedMetadata));

        ResponseEntity<?> response = annotationController.getAnnotation("UUID", "CAPONE", "e5feb4ba", EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
        assertContains("e5feb4ba", errorResponse);
        assertContains("UUID:CAPONE", errorResponse);

    }

    @Test
    public void testGetAnnotationExternalIdWithAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-d5uxna.msizfm.-oxy0iu");
        when(lookupService.executeLookupUUIDQuery(matches("UUID"), matches("CORLEONE"), any(), any(), any())).thenReturn(List.of(expectedMetadata));

        Annotation testAnnotation = generateCorleoneAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        ResponseEntity<?> response = annotationController.getAnnotation("UUID", "CORLEONE", expectedAnnotation.getAnnotationId(), EMPTY_HTTP_HEADERS,
                        defaultUserDetails);
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Test
    public void testUpdateAnnotationInternalId() throws Exception {
        configureImmediateAck();

        Annotation existingAnnotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());
        // simulate an update that masks/deletes the existing segment by submitting one with an empty values list
        Segment maskedSegment = existingAnnotation.getSegments(0).toBuilder().clearValues().build();
        Annotation updatedAnnotation = existingAnnotation.toBuilder().clearSegments().addSegments(maskedSegment).build();
        String body = AnnotationJsonUtils.annotationToJsonWithoutIds(updatedAnnotation);

        ResponseEntity<?> response = annotationController.updateAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno",
                        existingAnnotation.getAnnotationId(), body, EMPTY_HTTP_HEADERS, defaultUserDetails);

        assertResponseStatus(200, response);
        Annotation resultAnnotation = assertExpectedEntity(Annotation.class, response);
        assertTrue(resultAnnotation.getSegments(0).getValuesList().isEmpty(), "expected the masked segment's empty values list to be preserved");
        assertEquals(existingAnnotation.getAnnotationId(), resultAnnotation.getMetadataMap().get(AnnotationUtils.UPDATE_REFERENCE),
                        "expected the updated annotation's metadata to reference the annotation it updates");
        verify(annotationSink, times(1)).send(any());
    }

    @Test
    public void testUpdateAnnotationInternalIdMissingId() {
        ResponseEntity<?> response = annotationController.updateAnnotation("DOCUMENT", "20250704_249/testDataType/aaaaa.bbbbb.ccccc", "23BD91EC", "{}",
                        EMPTY_HTTP_HEADERS, defaultUserDetails);
        assertResponseStatus(500, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("Invalid annotation json", errorResponse);
    }

    @Test
    public void testUpdateAnnotationTargetNotFound() throws Exception {
        Annotation existingAnnotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());
        String body = AnnotationJsonUtils.annotationToJsonWithoutIds(existingAnnotation);

        // "aaaaaaaa" is not a real annotation id for this document, so the update should be rejected before anything is sent.
        ResponseEntity<?> response = annotationController.updateAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "aaaaaaaa", body,
                        EMPTY_HTTP_HEADERS, defaultUserDetails);

        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("aaaaaaaa", errorResponse);
        verify(annotationSink, times(0)).send(any());
    }

    @Test
    public void testGetAnnotationSegmentInternalId() {
        Metadata expectedMetadata = new Metadata("shard", "20250704_249", "testDataType", "abcde.fghij.klmno");
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);

        //@formatter:off
        ResponseEntity<?> response = annotationController.getAnnotationSegment(
                "DOCUMENT",
                expectedAnnotation.getShard() + "/" + expectedAnnotation.getDataType() + "/" + expectedAnnotation.getUid(),
                expectedAnnotation.getAnnotationId(),
                expectedAnnotation.getSegments(0).getSegmentHash(), EMPTY_HTTP_HEADERS, defaultUserDetails
        );
        //@formatter:on

        assertResponseStatus(200, response);
        Map<Metadata,Collection<Segment>> result = assertExpectedEntity(Map.class, response);
        assertFalse(result.isEmpty());
        assertEquals(1, result.size());
        Collection<Segment> segmentsList = result.get(expectedMetadata);
        assertNotNull(segmentsList);
        assertFalse(segmentsList.isEmpty());
        assertEquals(1, segmentsList.size());
        assertSegmentsEqual(expectedAnnotation.getSegmentsList(), segmentsList);
    }

    @Test
    public void testGetAnnotationSegmentInternalIdMissingAnnotationId() {
        //@formatter:off
        ResponseEntity<?> response = annotationController.getAnnotationSegment(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "aaaaaaaa",
                "5a7bcdd9", EMPTY_HTTP_HEADERS, defaultUserDetails);
        //@formatter:on
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("aaaaaaaa", errorResponse);
    }

    @Test
    public void testGetAnnotationSegmentInternalIdMissingSegmentHash() {
        //@formatter:off
        ResponseEntity<?> response = annotationController.getAnnotationSegment(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "23BD91EC",
                "bbbbbbbb", EMPTY_HTTP_HEADERS, defaultUserDetails);
        //@formatter:on
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No segments found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("bbbbbbbb", errorResponse);
    }

    /**
     * Assert that the response has the expected http status code.
     *
     * @param expected
     *            the expected http status code
     * @param response
     *            the response to check.
     */
    private static void assertResponseStatus(int expected, ResponseEntity<?> response) {
        log.debug("Response entity in assertResponseStatus: {}", response);
        assertEquals(expected, response.getStatusCode().value(),
                        String.format("Unexpected response http status: '%d', expected '%d'", response.getStatusCode().value(), expected));
    }

    /**
     * Assert that the response's entity has is a specific class, and return that entity cast to the specified class for convienience.
     *
     * @param clazz
     *            the class to check for.
     * @param response
     *            the response whose entity we'll be checking
     * @return the entity cast to the specified class.
     * @param <T>
     *            the type for the return, based on the specified class.
     */
    private static <T> T assertExpectedEntity(Class<T> clazz, ResponseEntity<?> response) {
        final Object entity = response.getBody();
        if (entity == null && clazz == null) {
            fail("Unsupported null entity and expected class");
        } else if (entity == null) {
            fail(String.format("Unexpected null entity, expected '%s'", clazz));
        } else if (clazz == null) {
            fail(String.format("Unexpected entity class: '%s' expected null", entity.getClass()));
        }

        assertTrue(clazz.isAssignableFrom(entity.getClass()), String.format("Unexpected entity class: '%s', expected '%s'", entity.getClass(), clazz));
        return clazz.cast(entity);
    }

    /**
     * Assert that the provided message contains the expected string.
     *
     * @param expected
     *            the expected substring
     * @param message
     *            the message to check.
     */
    private static void assertContains(String expected, String message) {
        assertTrue(message.contains(expected), String.format("Unexpected response: '%s', did not contain the string '%s'", message, expected));
    }
}
