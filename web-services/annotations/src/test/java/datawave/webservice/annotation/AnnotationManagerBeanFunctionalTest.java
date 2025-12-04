package datawave.webservice.annotation;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertSegmentsEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateMultiTestSegment;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.ejb.EJBContext;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.util.TableName;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutorBean;

@SuppressWarnings({"unused", "unchecked", "SpellCheckingInspection"})
@RunWith(Arquillian.class)
public class AnnotationManagerBeanFunctionalTest {
    protected static AccumuloClient client = null;

    private static final Logger log = Logger.getLogger(AnnotationManagerBeanFunctionalTest.class);

    // used for writing data for specific tests
    protected static AnnotationDataAccess testDao;

    @Mock
    @Produces
    private static EJBContext ctx;

    @Mock
    @Produces
    private static AccumuloConnectionFactory connectionFactory;

    @Mock
    @Produces
    private static QueryExecutorBean queryExecutorBean;

    @Mock
    @Produces
    private static QueryLogicFactory queryLogicFactory;

    @Mock
    @Produces
    private static UserOperations userOperations;

    @Mock
    private static AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Produces
    private static final ResponseObjectFactory responseObjectFactory = new DefaultResponseObjectFactory();

    @Inject
    @SpringBean(name = "AnnotationManager")
    protected AnnotationManager annotationManager;

    @Deployment
    public static JavaArchive createDeployment() {
        System.setProperty("cdi.bean.context", "annotationBeanRefContext.xml");

        //@formatter:off
        return ShrinkWrap.create(JavaArchive.class)
                .addPackages(
                        true,
                        "org.apache.deltaspike",
                        "io.astefanutti.metrics.cdi",
                        "datawave.query",
                        "org.jboss.logging",
                        "datawave.webservice.query.result.event"
                )
                .addClass(AnnotationManagerBean.class)
                .addClass(AccumuloConnectionRequestBean.class)
                .deleteClass(DefaultEdgeEventQueryLogic.class)
                .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                .addAsManifestResource(
                        new StringAsset(
                                "<alternatives>" +
                                        "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" +
                                        "</alternatives>"),
                        "beans.xml"
                );
        //@formatter:on
    }

    @BeforeClass
    public static void setUp() throws Exception {

        QueryTestTableHelper queryTestTableHelper = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        client = queryTestTableHelper.client;

        String tableName = "annotations";
        TableOperations tops = client.tableOperations();
        tops.create("annotations");

        VisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
        TimestampTransformer timestampTransformer = new DefaultTimestampTransformer();

        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
        Authorizations auths = new Authorizations("ALL", "PUBLIC");
        testDao = new AnnotationDataAccess(client, Set.of(auths), tableName, annotationSerializer);

        Annotation testAnnotation = generateTestAnnotation();
        testDao.addAnnotation(testAnnotation);

        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);

        addAnnotationTestData(client);

        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        PrintUtility.printTable(client, auths, tableName);

        ctx = EasyMock.createMock(EJBContext.class);

        //@formatter:off
        DatawaveUser user = new DatawaveUser(
                SubjectIssuerDNPair.of("testUser"),
                DatawaveUser.UserType.USER,
                List.of("ALL", "PUBLIC"),
                null,
                null,
                -1L
        );
        //@formatter:on

        DatawavePrincipal principal = new DatawavePrincipal(List.of(user));
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();

        connectionFactory = EasyMock.createMock(AccumuloConnectionFactory.class);
        EasyMock.expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(new HashMap<>()).anyTimes();

        //@formatter:off
        EasyMock.expect(
                connectionFactory.getClient(
                        EasyMock.anyObject(),
                        EasyMock.anyObject(),
                        EasyMock.anyObject(),
                        EasyMock.anyObject(),
                        EasyMock.anyObject()
                )
        ).andReturn(client).anyTimes();
        //@formatter:on

        queryExecutorBean = EasyMock.createMock(QueryExecutorBean.class);
        queryLogicFactory = EasyMock.createMock(QueryLogicFactory.class);
        userOperations = EasyMock.createMock(UserOperations.class);
        accumuloConnectionRequestBean = EasyMock.createMock(AccumuloConnectionRequestBean.class);

        EasyMock.replay(ctx, connectionFactory);
    }

    public static void addAnnotationTestData(AccumuloClient client) {
        testDao.addAnnotation(generateCorleoneAnnotation());
    }

    public static Annotation generateCorleoneAnnotation() {
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
                .addAllSegments(List.of(generateMultiTestSegment()))
                .putAllMetadata(metadata)
                .build();
        //@formatter:on
    }

    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);
        AnnotationManagerBean bean = (AnnotationManagerBean) annotationManager;
        bean.setEJBContext(ctx);
    }

    @Test
    public void testGetAnnotationTypesInternalId() {
        Metadata expectedMetadata = new Metadata("shard", "20250704_249", "testDataType", "abcde.fghij.klmno");
        Response response = annotationManager.getAnnotationTypes("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");
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
        Response response = annotationManager.getAnnotationTypes("DOCUMENT", "20250704_249/testDataType/12345.67890.12345");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotation types found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/12345.67890.12345", errorResponse);
    }

    @Test
    public void testGetAnnotationTypesExternalIdNoAnnotations() {
        Response response = annotationManager.getAnnotationTypes("UUID", "CAPONE");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotation types found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
    }

    @Test
    public void testGetAnnotationTypesExternalIdWithAnnotations() {
        Metadata expectedMetadata = new Metadata("shard", "20130101_0", "test", "-d5uxna.msizfm.-oxy0iu");
        Response response = annotationManager.getAnnotationTypes("UUID", "CORLEONE");
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
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Response response = annotationManager.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAnnotationsForMissingInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Response response = annotationManager.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/12345.67890.12345");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/12345.67890.12345", errorResponse);
    }

    @Test
    public void testGetAnnotationsForExternalIdNoAnnotations() {
        Response response = annotationManager.getAnnotationsFor("UUID", "CAPONE");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
    }

    @Test
    public void testAnnotationsForExternalIdWithAnnotations() {
        Annotation testAnnotation = generateCorleoneAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Response response = annotationManager.getAnnotationsFor("UUID", "CORLEONE");
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAllAnnotationsByTypeInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        // TODO: insert a second annotation for the same document with a different type?
        //@formatter:off
        Response response = annotationManager.getAnnotationsByType(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "testAnnotationType"
        );
        //@formatter:on
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAllAnnotationByTypeInternalIdMissingType() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        // TODO: insert a second annotation for the same document with a different type?
        //@formatter:off
        Response response = annotationManager.getAnnotationsByType(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "missingType"
        );
        //@formatter:on
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations of type found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("missingType", errorResponse);
    }

    @Test
    public void testGetAllAnnotationByTypeExternalIdNoAnnotations() {
        Response response = annotationManager.getAnnotationsByType("UUID", "CAPONE", "testAnnotationType");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations of type found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
        assertContains("UUID:CAPONE", errorResponse);
        assertContains("testAnnotationType", errorResponse);

    }

    @Test
    public void testGetAllAnnotationByTypeExternalIdWithAnnotations() {
        Annotation testAnnotation = generateCorleoneAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Response response = annotationManager.getAnnotationsByType("UUID", "CORLEONE", "corleoneAnnotationType");
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAnnotationInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Response response = annotationManager.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "a75beb9e");
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Test
    public void testGetAnnotationMissingInternalId() {
        Response response = annotationManager.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "aaaaaaaa");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("aaaaaaaa", errorResponse);

    }

    @Test
    public void testGetAnnotationExternalIdNoAnnotations() {
        Response response = annotationManager.getAnnotation("UUID", "CAPONE", "e5feb4ba");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20130101_0/test/-cvy0gj.tlf59s.-duxzua", errorResponse);
        assertContains("e5feb4ba", errorResponse);
        assertContains("UUID:CAPONE", errorResponse);

    }

    @Test
    public void testGetAnnotationExternalIdWithAnnotations() {
        Annotation testAnnotation = generateCorleoneAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        Response response = annotationManager.getAnnotation("UUID", "CORLEONE", "2e8fbb3e");
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Ignore
    public void testUpdateAnnotationInternalId() {
        fail("Not implemented");
    }

    @Ignore
    public void testUpdateAnnotationInternalIdMissingId() {
        fail("Not implemented");
    }

    @Test
    public void testGetAnnotationSegmentInternalId() {
        Metadata expectedMetadata = new Metadata("shard", "20250704_249", "testDataType", "abcde.fghij.klmno");
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAnnotationAndSegmentIds(testAnnotation);
        //@formatter:off
        Response response = annotationManager.getAnnotationSegment(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "a75beb9e",
                "5a7bcdd9"
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
        Response response = annotationManager.getAnnotationSegment(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "aaaaaaaa",
                "5a7bcdd9");
        //@formatter:on
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotations found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("aaaaaaaa", errorResponse);
    }

    @Test
    public void testGetAnnotationSegmentInternalIdMissingSegmentId() {
        //@formatter:off
        Response response = annotationManager.getAnnotationSegment(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "a75beb9e",
                "bbbbbbbb");
        //@formatter:on
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No segments found for identifier", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
        assertContains("bbbbbbbb", errorResponse);
    }

    @Ignore
    public void testAddSegmentInternalId() {
        fail("Not implemented");
    }

    @Ignore
    public void testUpdateSegmentInternalId() {
        fail("Not implemented");
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    /**
     * Assert that the response has the expected http status code.
     *
     * @param expected
     *            the expected http status code
     * @param response
     *            the response to check.
     */
    private static void assertResponseStatus(int expected, Response response) {
        assertEquals(expected, response.getStatus(), String.format("Unexpected response http status: '%d', expected '%d'", response.getStatus(), expected));
    }

    /**
     * Assert that the response's entity has is a specific class, and return that entity cast to the specified class for convienience.
     *
     * @param clazz
     *            the class to check for.
     * @param response
     *            the repsonse whose entity we'll be checking
     * @return the entity cast to the specified class.
     * @param <T>
     *            the type for the return, based on the specified class.
     */
    private static <T> T assertExpectedEntity(Class<T> clazz, Response response) {
        final Object entity = response.getEntity();
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
