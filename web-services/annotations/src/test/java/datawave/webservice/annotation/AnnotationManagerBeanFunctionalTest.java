package datawave.webservice.annotation;

import static datawave.annotation.protobuf.v1.BoundaryType.TIME_MILLI;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationListsEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationsEqual;
import static datawave.annotation.test.v1.AnnotationAssertions.assertSegmentsEqual;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateMultiTestSegment;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotationSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ejb.EJBContext;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
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
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutorBean;

@SuppressWarnings({"unused", "unchecked", "SpellCheckingInspection"})
@RunWith(Arquillian.class)
public class AnnotationManagerBeanFunctionalTest {
    protected static AccumuloClient client = null;

    private static ManagedExecutorService federatedReadExecutor;

    private static final Logger log = Logger.getLogger(AnnotationManagerBeanFunctionalTest.class);

    // used for writing data for specific tests
    protected static AnnotationDataAccess testAnnotationDao;
    protected static AnnotationDataAccess testTruthmarkDao;

    @Mock
    @Produces
    private EJBContext ctx;

    @Mock
    @Produces
    private AccumuloConnectionFactory connectionFactory;

    @Mock
    @Produces
    private QueryExecutorBean queryExecutorBean;

    @Mock
    @Produces
    private QueryLogicFactory queryLogicFactory;

    @Mock
    @Produces
    private UserOperations userOperations;

    @Mock
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Produces
    private static final ResponseObjectFactory responseObjectFactory = new DefaultResponseObjectFactory();

    @Inject
    @SpringBean(name = "AnnotationManagerConfig")
    protected AnnotationManagerConfig annotationManagerConfig;

    protected AnnotationManager annotationManager;
    protected DatawavePrincipal defaultPrincipal;

    @Deployment
    public static JavaArchive createDeployment() {
        System.setProperty("datawave.configuration.spring.useBootstrapContext", "false");
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
    public static void setupTestData() throws Exception {

        QueryTestTableHelper queryTestTableHelper = new QueryTestTableHelper(AnnotationManagerBeanFunctionalTest.class.toString(), log);
        client = queryTestTableHelper.client;

        String annotationTableName = "annotation";
        String annotationSourceTableName = "annotationSource";
        String truthmarkTableName = "truthmark";
        String truthmarkSourceTableName = "truthmarkSource";

        TableOperations tops = client.tableOperations();
        tops.create(annotationTableName);
        tops.create(annotationSourceTableName);
        tops.create(truthmarkTableName);
        tops.create(truthmarkSourceTableName);

        VisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();
        TimestampTransformer timestampTransformer = new DefaultTimestampTransformer();

        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
        AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer(visibilityTransformer, timestampTransformer);

        Authorizations auths = new Authorizations("ALL", "PUBLIC", "PRIVATE");
        testAnnotationDao = new AnnotationDataAccess(client, Set.of(auths), annotationTableName, annotationSourceTableName, annotationSerializer,
                        annotationSourceSerializer);
        testTruthmarkDao = new AnnotationDataAccess(client, Set.of(auths), truthmarkTableName, truthmarkSourceTableName, annotationSerializer,
                        annotationSourceSerializer);

        // add some annotation test data
        Annotation testAnnotation = generateTestAnnotation();
        testAnnotationDao.addAnnotation(testAnnotation);

        AnnotationSource testAnnotationSource = generateTestAnnotationSource();
        testAnnotationDao.addAnnotationSource(testAnnotationSource);

        // add some truthmark test data
        Annotation truthmarkAnnotation = getTruthmarkAnnotation(testAnnotation);
        testTruthmarkDao.addAnnotation(truthmarkAnnotation);

        // add the wiseguys data
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);

        // add more annotation test data
        addMoreAnnotationTestData(client);

        // dump the contents of the various tables used for this test
        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        PrintUtility.printTable(client, auths, annotationTableName);
        PrintUtility.printTable(client, auths, annotationSourceTableName);
        PrintUtility.printTable(client, auths, truthmarkTableName);
        PrintUtility.printTable(client, auths, truthmarkSourceTableName);

        // set up the executor used for federated reads.
        final ExecutorService federatedReadExecutorDelegate = Executors.newCachedThreadPool();
        federatedReadExecutor = new DelegatingManagedExecutorService(federatedReadExecutorDelegate);
    }

    private static @NonNull Annotation getTruthmarkAnnotation(Annotation testAnnotation) {
        //@formatter:off
        AnnotationSource partialTruthmarkSource = AnnotationSource.newBuilder()
                .setEngine("human")
                .setModel("john")
                .setPlatform("truthmark")
                .putMetadata("visibility", "PUBLIC")
                .putMetadata("created_date","2025-10-02T00:00:00Z")
                .build();

        SegmentValue truthmarkSegmentValue = SegmentValue.newBuilder()
                .setValue("cat").setScore(1.0f)
                .build();

        SegmentBoundary truthmarkBoundery = SegmentBoundary.newBuilder()
                .setBoundaryType(TIME_MILLI)
                .setStart(1540).setEnd(5200)
                .build();

        Segment truthmarkSegment = Segment.newBuilder()
                .addValues(truthmarkSegmentValue)
                .setBoundary(truthmarkBoundery)
                .build();

        AnnotationSource truthmarkSource = AnnotationUtils.injectAnnotationSourceHashes(partialTruthmarkSource);
        Annotation truthmarkAnnotation = testAnnotation.toBuilder()
                .setSource(truthmarkSource)
                .setAnalyticSourceHash(truthmarkSource.getAnalyticSourceHash())
                .clearSegments()
                .addSegments(truthmarkSegment)
                .clearMetadata()
                .putMetadata("visibility", "PUBLIC")
                .putMetadata("created_date","2025-10-02T00:00:00Z")
                .build();
        //@formatter:on
        return truthmarkAnnotation;
    }

    @Before
    public void setup() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);

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

        defaultPrincipal = new DatawavePrincipal(List.of(user));
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(defaultPrincipal).times(1);

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

        connectionFactory.returnClient(EasyMock.anyObject());
        EasyMock.expectLastCall().anyTimes();

        queryExecutorBean = EasyMock.createMock(QueryExecutorBean.class);
        queryLogicFactory = EasyMock.createMock(QueryLogicFactory.class);
        userOperations = EasyMock.createMock(UserOperations.class);
        accumuloConnectionRequestBean = EasyMock.createMock(AccumuloConnectionRequestBean.class);

        EasyMock.replay(ctx, connectionFactory);

        annotationManager = new AnnotationManagerBean();

        // Inject mocks and config into private fields
        setField(annotationManager, "ctx", ctx);
        setField(annotationManager, "connectionFactory", connectionFactory);
        setField(annotationManager, "accumuloConnectionRequestBean", accumuloConnectionRequestBean);
        setField(annotationManager, "config", annotationManagerConfig);
        setField(annotationManager, "responseObjectFactory", responseObjectFactory);
        setField(annotationManager, "annotationFederatedReadExecutor", federatedReadExecutor);

    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    public static void addMoreAnnotationTestData(AccumuloClient client) {
        // TODO: add annotation source data
        testAnnotationDao.addAnnotation(generateCorleoneAnnotation());
        testAnnotationDao.addAnnotation(generatePrivateAnnotation());
        testAnnotationDao.addAnnotation(generateUpdatableAnnotation());
    }

    public static Annotation generateCorleoneAnnotation() {
        return generateAnnotationWithId("CORLEONE", "corleone");
    }

    public static Annotation generatePrivateAnnotation() {
        Annotation baseAnnotation = generateAnnotationWithId("SOPRANO", "soprano");

        Map<String,String> metadata = new HashMap<>();
        metadata.put("UUID", "SOPRANO");
        metadata.put("visibility", "PRIVATE");
        metadata.put("created_date", "2025-10-02T00:00:00.000Z");

        //@formatter:off
        return baseAnnotation.toBuilder()
                .setShard("20130101_0")
                .setDataType("test")
                .setUid("-1kfeoq.-80b5fs.r0262j")
                .setAnnotationType("sopranoAnnotationType")
                .setDocumentId("SOPRANO")
                .clearSegments()
                .addAllSegments(List.of(generateMultiTestSegment()))
                .clearMetadata()
                .putAllMetadata(metadata)
                .build();
        //@formatter:on
    }

    public static Annotation generateUpdatableAnnotation() {
        Annotation baseAnnotation = generateAnnotationWithId("ANDOLINI", "anodlini");

        Map<String,String> metadata = new HashMap<>();
        metadata.put("UUID", "ANDOLINI");
        metadata.put("visibility", "PUBLIC");
        metadata.put("created_date", "2025-10-03T00:00:00.000Z");

        //@formatter:off
        return baseAnnotation.toBuilder()
                .setShard("20130101_0")
                .setDataType("test")
                .setUid("-d5uxna.msizfm.-oxy0iu.1")
                .setAnnotationType("anodlinuAnnotationType")
                .setDocumentId("ANDOLINI")
                .clearMetadata()
                .putAllMetadata(metadata)
                .build();
        //@formatter:on
    }

    public static Annotation generateAnnotationWithId(String id, String type) {
        AnnotationSource baseAnnotationSource = generateTestAnnotationSource();
        AnnotationSource annotationSource = AnnotationUtils.injectAnnotationSourceHashes(baseAnnotationSource);
        Map<String,String> metadata = new HashMap<>();
        metadata.put("UUID", id);
        metadata.put("visibility", "ALL");
        metadata.put("created_date", "2025-10-01T00:00:00.000Z");

        //@formatter:off
        return Annotation.newBuilder()
                .setShard("20130101_0")
                .setDataType("test")
                .setUid("-d5uxna.msizfm.-oxy0iu")
                .setAnnotationType(type + "AnnotationType")
                .setDocumentId(id)
                .setSource(annotationSource)
                .addAllSegments(List.of(generateMultiTestSegment()))
                .putAllMetadata(metadata)
                .build();
        //@formatter:on
    }

    @Test
    public void testGetAnnotationSource() {
        Response response = annotationManager.getAnnotationSource("52EF0E07742AC65873C6DF80759AF192");
        assertResponseStatus(200, response);
        AnnotationSource annotationSource = assertExpectedEntity(AnnotationSource.class, response);
        assertNotNull(annotationSource);
    }

    @Test
    public void testGetMissingAnnotationSource() {
        Response response = annotationManager.getAnnotationSource("52EF0E07742AC65873C6DF80759AF193");
        assertResponseStatus(404, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("No annotation source found for analyticHash", errorResponse);
        assertContains("52EF0E07742AC65873C6DF80759AF193", errorResponse);

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
        Annotation expectedTestAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Annotation testTruthmark = getTruthmarkAnnotation(testAnnotation);
        Annotation expectedTestTruthmark = AnnotationUtils.injectAllHashes(testTruthmark);
        List<Annotation> expectedAnnotations = List.of(expectedTestAnnotation, expectedTestTruthmark);

        Response response = annotationManager.getAnnotationsFor("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertAnnotationListsEqual(expectedAnnotations, annotationList);
    }

    @Test
    public void testGetAnnotationsForMissingInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
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
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotationsFor("UUID", "CORLEONE");
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAllAnnotationsByTypeInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedTestAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Annotation testTruthmark = getTruthmarkAnnotation(testAnnotation);
        Annotation expectedTestTruthmark = AnnotationUtils.injectAllHashes(testTruthmark);
        List<Annotation> expectedAnnotations = List.of(expectedTestAnnotation, expectedTestTruthmark);

        // TODO: insert an additional annotation for the same document with a different type?
        //@formatter:off
        Response response = annotationManager.getAnnotationsByType(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "testAnnotationType"
        );
        //@formatter:on
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertAnnotationListsEqual(expectedAnnotations, annotationList);
    }

    @Test
    public void testGetAllAnnotationByTypeInternalIdMissingType() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
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
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotationsByType("UUID", "CORLEONE", "corleoneAnnotationType");
        assertResponseStatus(200, response);
        ArrayList<Annotation> annotationList = assertExpectedEntity(ArrayList.class, response);
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.get(0));
    }

    @Test
    public void testGetAnnotationInternalId() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "23BD91EC");
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Test
    public void testGetPrivateAnnotationFailure() {
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotation("DOCUMENT", "20130102_0/test/-a4vymb.ntjagn.-pyz1jv", "A22496BE");
        assertResponseStatus(404, response);
    }

    @Test
    public void testGetPrivateAnnotationSuccess() {
        //@formatter:off
        DatawaveUser user = new DatawaveUser(
                SubjectIssuerDNPair.of("testUser"),
                DatawaveUser.UserType.USER,
                List.of("ALL", "PUBLIC","PRIVATE"),
                null,
                null,
                -1L
        );
        //@formatter:on

        DatawavePrincipal principal = new DatawavePrincipal(List.of(user));
        EasyMock.reset(ctx);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).times(1);
        EasyMock.replay(ctx);

        Annotation testAnnotation = generatePrivateAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotation("DOCUMENT", "20130101_0/test/-1kfeoq.-80b5fs.r0262j", "A22496BE");
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Test
    public void testValidatePrivateAuthorizationReset() {
        Annotation testAnnotation = generatePrivateAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);

        // validates that authorizations are reset across multiple calls.
        //@formatter:off
        DatawaveUser privateUser = new DatawaveUser(
                SubjectIssuerDNPair.of("testUser"),
                DatawaveUser.UserType.USER,
                List.of("ALL", "PUBLIC","PRIVATE"),
                null,
                null,
                -1L
        );
        //@formatter:on

        DatawavePrincipal privatePrincipal = new DatawavePrincipal(List.of(privateUser));
        EasyMock.reset(ctx);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(privatePrincipal).times(1);
        EasyMock.replay(ctx);

        Response privateResponse = annotationManager.getAnnotation("DOCUMENT", "20130101_0/test/-1kfeoq.-80b5fs.r0262j", "A22496BE");
        assertResponseStatus(200, privateResponse);
        List<Annotation> privateAnnotationList = assertExpectedEntity(List.class, privateResponse);
        assertFalse(privateAnnotationList.isEmpty());
        assertEquals(1, privateAnnotationList.size());
        assertAnnotationsEqual(expectedAnnotation, privateAnnotationList.iterator().next());

        //@formatter:off
        DatawaveUser publicUser = new DatawaveUser(
                SubjectIssuerDNPair.of("testUser"),
                DatawaveUser.UserType.USER,
                List.of("ALL", "PUBLIC"),
                null,
                null,
                -1L
        );
        //@formatter:on

        DatawavePrincipal publicPrincipal = new DatawavePrincipal(List.of(publicUser));
        EasyMock.reset(ctx);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(publicPrincipal).times(1);
        EasyMock.replay(ctx);

        Response publicannotationResponse = annotationManager.getAnnotation("DOCUMENT", "20130102_0/test/-a4vymb.ntjagn.-pyz1jv", "A9F9A0B4");
        assertResponseStatus(404, publicannotationResponse);
    }

    @Test
    public void testGetAnnotationInternalIdDisabled() {
        AnnotationManagerBean bean = (AnnotationManagerBean) annotationManager;
        AnnotationManagerConfig config = bean.getConfig();
        config.setEnableInternalIdLookup(false);

        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotation("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno", "23BD91EC");
        assertResponseStatus(500, response);
        String errorResponse = assertExpectedEntity(String.class, response);
        assertContains("Internal identifier lookup is disabled for", errorResponse);
        assertContains("20250704_249/testDataType/abcde.fghij.klmno", errorResponse);
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
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        Response response = annotationManager.getAnnotation("UUID", "CORLEONE", expectedAnnotation.getAnnotationId());
        assertResponseStatus(200, response);
        List<Annotation> annotationList = assertExpectedEntity(List.class, response);
        assertFalse(annotationList.isEmpty());
        assertEquals(1, annotationList.size());
        assertAnnotationsEqual(expectedAnnotation, annotationList.iterator().next());
    }

    @Test
    public void testGetAnnotationSegmentInternalId() {
        Metadata expectedMetadata = new Metadata("shard", "20250704_249", "testDataType", "abcde.fghij.klmno");
        Annotation testAnnotation = generateTestAnnotation();
        Annotation expectedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);

        //@formatter:off
        Response response = annotationManager.getAnnotationSegment(
                "DOCUMENT",
                expectedAnnotation.getShard() + "/" + expectedAnnotation.getDataType() + "/" + expectedAnnotation.getUid(),
                expectedAnnotation.getAnnotationId(),
                expectedAnnotation.getSegments(0).getSegmentHash()
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
    public void testGetAnnotationSegmentInternalIdMissingSegmentHash() {
        //@formatter:off
        Response response = annotationManager.getAnnotationSegment(
                "DOCUMENT",
                "20250704_249/testDataType/abcde.fghij.klmno",
                "23BD91EC",
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
        if (federatedReadExecutor != null) {
            federatedReadExecutor.shutdownNow();
        }
        TypeRegistry.reset();
    }

    /**
     * Minimal ManagedExecutorService wrapper for tests that need a concrete executor.
     */
    private static final class DelegatingManagedExecutorService implements ManagedExecutorService {
        private final ExecutorService delegate;

        private DelegatingManagedExecutorService(ExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return delegate.submit(task);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return delegate.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return delegate.submit(task, result);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return delegate.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return delegate.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
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
