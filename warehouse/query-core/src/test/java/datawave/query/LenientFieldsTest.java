package datawave.query;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
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
import org.junit.Test;
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
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.test.JexlNodeAssert;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * Loads some data in a mock accumulo table and then issues queries against the table using the shard query table.
 *
 */
public abstract class LenientFieldsTest {

    @RunWith(Arquillian.class)
    public static class ShardRange extends LenientFieldsTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
            ModelAdditions.addLenientModelEntries(client);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(List<String> expected, String plan, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQuery(expected, plan, querystr, startDate, endDate, extraParms, client);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends LenientFieldsTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            ModelAdditions.addLenientModelEntries(client);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(List<String> expected, String plan, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQuery(expected, plan, querystr, startDate, endDate, extraParms, client);
        }
    }

    private static class ModelAdditions extends WiseGuysIngest {
        private static void addLenientModelEntries(AccumuloClient client) throws TableNotFoundException, MutationsRejectedException {
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
            Mutation mutation = null;
            BatchWriter bw = null;
            try {
                bw = client.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);

                mutation = new Mutation("NAM");
                mutation.put("DATAWAVE", "MAGIC" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
                mutation.put("DATAWAVE", "lenient" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("AG");
                mutation.put("DATAWAVE", "lenient" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
        }
    }

    private static final Logger log = Logger.getLogger(LenientFieldsTest.class);

    protected Authorizations auths = new Authorizations("ALL");

    protected Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");

    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        logic.setFullTableScanEnabled(true);

        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQuery(List<String> expected, String plan, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws Exception;

    protected void runTestQuery(List<String> expected, String plan, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    AccumuloClient client) throws Exception {
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
        logic.setFullTableScanEnabled(true);

        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);
        JexlNodeAssert.assertThat(JexlASTHelper.parseJexlQuery(config.getQueryString())).isEqualTo(plan);

        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();

            log.debug(entry.getKey() + " => " + d);

            Attribute<?> attr = d.get("UUID.0");

            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatawaveTypeAttribute, was: " + attr.getClass().getName(),
                            attr instanceof TypeAttribute || attr instanceof PreNormalizedAttribute);

            TypeAttribute<?> UUIDAttr = (TypeAttribute<?>) attr;

            String UUID = UUIDAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID for query:" + querystr + "  " + UUID, expected.contains(UUID));

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
        Assert.assertEquals("Unexpected number of records for query:" + querystr, expected.size(), resultSet.size());
    }

    @Test
    public void testLenientFields() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("lenient.fields", "ETA,AGE,MAGIC,NOME,NAME,NAM,AG");

        if (log.isDebugEnabled()) {
            log.debug("testLenientFields");
        }
        // @formatter:off
        String[] queryStrings = {
                "AG > 'abc10'",
                "AG == '40'",
                "NAM > '40'",
                "NAM == 'abc40'",
        };
        String[] expectedPlans = {
                "(((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'ETA > \\'abc10\\''))) || ((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'AGE > \\'abc10\\''))))",
                "(ETA == '+bE4' || AGE == '+bE4')",
                "(MAGIC > '+bE4' || NOME > '40' || NAME > '40')",
                "(NAME == 'abc40' || NOME == 'abc40' || ((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'MAGIC == \\'abc40\\''))))",
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList(),
                Arrays.asList("CORLEONE", "CAPONE"),
                Arrays.asList("CORLEONE", "SOPRANO", "CAPONE", "TATTAGLIA"),
                Arrays.asList()
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], expectedPlans[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
}
