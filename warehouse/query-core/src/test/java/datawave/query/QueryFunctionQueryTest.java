package datawave.query;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * Integration test for {@link QueryFunctions}.
 * <p>
 * The following functions are tested
 * <ul>
 * <li>{@link QueryFunctions#INCLUDE_TEXT}</li>
 * <li>{@link QueryFunctions#MATCH_REGEX}</li>
 * </ul>
 */
public abstract class QueryFunctionQueryTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @RunWith(Arquillian.class)
    public static class ShardRange extends QueryFunctionQueryTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            // this will get property substituted into the TypeMetadataBridgeContext.xml file
            // for the injection test (when this unit test is first created)
            File tempDir = temporaryFolder.newFolder("TempDirForCompositeFunctionsTestShardRange");
            System.setProperty("type.metadata.dir", tempDir.getCanonicalPath());

            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.ShardRange.class.toString(), log);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @AfterClass
        public static void teardown() {
            TypeRegistry.reset();
        }

        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, client, eventQueryLogic);
        }

        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, ShardQueryLogic logic)
                        throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, client, logic);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends QueryFunctionQueryTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            // this will get property substituted into the TypeMetadataBridgeContext.xml file
            // for the injection test (when this unit test is first created)
            File tempDir = temporaryFolder.newFolder("TempDirForCompositeFunctionsTestDocumentRange");
            System.setProperty("type.metadata.dir", tempDir.getCanonicalPath());

            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.DocumentRange.class.toString(), log);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @AfterClass
        public static void teardown() {
            TypeRegistry.reset();
        }

        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, client, eventQueryLogic);
        }

        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, ShardQueryLogic logic)
                        throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, client, logic);
        }
    }

    private static final Logger log = Logger.getLogger(CompositeFunctionsTest.class);

    protected Authorizations auths = new Authorizations("ALL");

    private final Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic eventQueryLogic;

    private KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");

    @Deployment
    public static JavaArchive createDeployment() {

        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event", "datawave.core.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception;

    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    ShardQueryLogic logic) throws Exception;

    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, AccumuloClient client,
                    ShardQueryLogic logic) throws Exception {
        log.debug("runTestQuery");
        log.trace("Creating QueryImpl");
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        logic.setMaxEvaluationPipelines(1);

        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();

            log.debug(entry.getKey() + " => " + d);

            Attribute<?> attr = d.get("UUID");
            if (attr == null) {
                attr = d.get("UUID.0");
            }

            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(),
                            attr instanceof TypeAttribute || attr instanceof PreNormalizedAttribute);

            TypeAttribute<?> UUIDAttr = (TypeAttribute<?>) attr;

            String UUID = UUIDAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID: " + UUID, expected.contains(UUID));

            resultSet.add(UUID);
            docs.add(d);
        }

        if (expected.size() > resultSet.size()) {
            expectedSet.addAll(expected);
            expectedSet.removeAll(resultSet);

            for (String s : expectedSet) {
                log.warn("Missing: " + s);
            }
        }

        if (!expected.containsAll(resultSet)) {
            log.error("Expected results " + expected + " differ form actual results " + resultSet);
        }
        Assert.assertTrue("Expected results " + expected + " differ form actual results " + resultSet, expected.containsAll(resultSet));
        Assert.assertEquals("Unexpected number of records", expected.size(), resultSet.size());
    }

    @Test
    public void testIncludeText() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("hit.list", "true");

        // @formatter:off
        String[] queryStrings = {
                        "UUID == 'corleone' && f:includeText(GENERE, 'FEMALE')",
                        "UUID == 'corleone' && f:includeText(GENERE, 'male')",
                        "UUID == 'corleone' && f:includeText(NUMBER, '25')",
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                        List.of("CORLEONE"),
                        List.of(),   //  misses because includeText is case-sensitive
                        List.of("CORLEONE"),
        };
        // @formatter:on

        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testMatchRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("hit.list", "true");

        // @formatter:off
        String[] queryStrings = {
                        "UUID == 'corleone' && f:matchRegex(GENERE, '.*MALE')",
                        "UUID == 'corleone' && f:matchRegex(GENERE, '.*male')",
                        "UUID == 'corleone' && f:matchRegex(NUMBER, '2.*')",
                        "UUID == 'corleone' && f:matchRegex(GENERE, '[A-Z]+')",
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                        List.of("CORLEONE"),
                        List.of("CORLEONE"),
                        List.of("CORLEONE"),
                        List.of("CORLEONE"),
        };
        // @formatter:on

        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
}
