package datawave.query;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.util.ScannerHelper;
import datawave.test.HitTermAssertions;
import datawave.util.TableName;

/**
 *
 * This test confirms that hit terms are found in the correct documents, and only in the correct documents. The test data has fields that will hit in different
 * grouping context levels, and assures that the hits contain the fields with the correct grouping context. It also confirms that an 'or' query that hits
 * different fields in the returned documents will have the correct hit terms.
 * <p>
 * The same tests are made against document ranges and shard ranges
 * <p>
 * If this test fails, then hit terms are broken... maybe... probably...
 */
public abstract class IfThisTestFailsThenHitTermsAreBroken {

    @ClassRule
    // Temporary folders are not successfully deleted in this test with @Rule for some reason, but they are with @ClassRule.
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    enum WhatKindaRange {
        SHARD, DOCUMENT
    }

    private static final Logger log = Logger.getLogger(IfThisTestFailsThenHitTermsAreBroken.class);

    protected static AccumuloClient client = null;

    protected static final Authorizations auths = new Authorizations("A");

    protected Set<Authorizations> authSet = Collections.singleton(auths);

    protected ShardQueryLogic logic = null;

    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");

    private String query;
    private int expectedResultCount = 0;
    private final Set<Document> results = new HashSet<>();
    private final Map<String,String> extraParameters = new HashMap<>();
    private final HitTermAssertions hitTermAssertions = new HitTermAssertions();

    @RunWith(Arquillian.class)
    public static class ShardRangeTest extends IfThisTestFailsThenHitTermsAreBroken {

        @BeforeClass
        public static void beforeAll() throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRangeTest.class.toString(), log);
            client = qtth.client;

            MoreTestData.writeItAll(client, WhatKindaRange.SHARD);
            if (log.isDebugEnabled()) {
                log.debug("testWithShardRange");
                PrintUtility.printTable(client, auths, TableName.SHARD);
                PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
                PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
            }
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRangeTest extends IfThisTestFailsThenHitTermsAreBroken {

        @BeforeClass
        public static void beforeAll() throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRangeTest.class.toString(), log);
            client = qtth.client;

            MoreTestData.writeItAll(client, WhatKindaRange.DOCUMENT);
            if (log.isDebugEnabled()) {
                log.debug("testWithDocumentRange");
                PrintUtility.printTable(client, auths, TableName.SHARD);
                PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
                PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
            }
        }
    }

    @Before
    public void beforeEach() {
        extraParameters.put("hit.list", "true");
        query = null;
        expectedResultCount = 0;
    }

    @After
    public void after() {
        TypeRegistry.reset();
        System.clearProperty("type.metadata.dir");

        extraParameters.clear();
        results.clear();
        hitTermAssertions.resetState();
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Before
    public void setup() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        File tempDir = temporaryFolder.newFolder();
        System.setProperty("type.metadata.dir", tempDir.getAbsolutePath());
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D,T,U,V,W,X,Y,Z");
        log.info("using tempFolder " + tempDir);

        logic = new ShardQueryLogic();
        logic.setMetadataTableName(QueryTestTableHelper.MODEL_TABLE_NAME);
        logic.setTableName(TableName.SHARD);
        logic.setIndexTableName(TableName.SHARD_INDEX);
        logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
        logic.setMaxResults(5000);
        logic.setMaxWork(25000);
        logic.setModelTableName(QueryTestTableHelper.MODEL_TABLE_NAME);
        logic.setQueryPlanner(new DefaultQueryPlanner());
        logic.setIncludeGroupingContext(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setMaxEvaluationPipelines(1);
        deserializer = new KryoDocumentDeserializer();
    }

    public void dumpTable(String tableName) throws Exception {
        Scanner s = ScannerHelper.createScanner(client, tableName, authSet);
        Range r = new Range();
        s.setRange(r);
        for (Entry<Key,Value> entry : s) {
            if (log.isDebugEnabled()) {
                log.debug(entry.getKey() + " " + entry.getValue());
            }
        }
    }

    private void drive() throws Exception {
        // might be called multiple times by a single test, so clear results
        results.clear();

        setupQuery();
        executeQuery();
        assertResults();
    }

    private void setupQuery() throws Exception {
        log.debug("runTestQuery");
        log.trace("Creating QueryImpl");
        QueryImpl settings = new QueryImpl();

        settings.setBeginDate(format.parse("20091231"));
        settings.setEndDate(format.parse("20150101"));
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(query);
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);
    }

    private void executeQuery() {
        for (Entry<Key,Value> entry : logic) {
            Document document = deserializer.apply(entry).getValue();
            results.add(document);
        }
    }

    private void assertResults() {
        Assert.assertEquals(expectedResultCount, results.size());
        for (Document result : results) {
            assertResult(result);
        }
    }

    private void assertResult(Document result) {
        boolean validated = hitTermAssertions.assertHitTerms(result);
        Assert.assertEquals(hitTermAssertions.hitTermExpected(), validated);
    }

    @Test
    public void testUnion() throws Exception {
        // sanity check that expects two documents
        query = "UUID == 'First' || UUID == 'Second'";
        hitTermAssertions.withRequiredAnyOf("UUID.0:First", "UUID.0:Second");
        expectedResultCount = 2;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testDoubleNestedUnion() throws Exception {
        // look for FOO or BAR, expecting the hit_terms to be in the right places
        query = "( UUID == 'First' || UUID == 'Second' ) && ( FOO == 'FOO' || BAR == 'BAR' )";
        hitTermAssertions.withRequiredAnyOf("UUID.0:First", "UUID.0:Second");
        hitTermAssertions.withRequiredAnyOf("FOO.0:FOO", "BAR.0:BAR");
        expectedResultCount = 2;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testNestedUnionWithAnchorOfDifferentGroupingContext() throws Exception {
        // should find NAME0 in different grouping contexts, but the hit terms should be correct
        query = "( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'NAME0'";
        hitTermAssertions.withRequiredAnyOf("UUID.0:First", "UUID.0:Second");
        hitTermAssertions.withRequiredAnyOf("NAME.0:NAME0", "NAME.1:NAME0");
        expectedResultCount = 2;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testNestedUnionFirstDocumentReturned() throws Exception {
        // this may get initial hits in Second, but will return only First. Makes sure that hits from Second are not included in First
        query = "( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'Haiqu' && FOO == 'FOO'";
        hitTermAssertions.withRequiredAllOf("UUID.0:First", "NAME.2:Haiqu", "FOO.0:FOO");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testNestedUnionSecondDocumentReturned() throws Exception {
        // this may get initial hits in First, but will return only Second. Makes sure that hits from First are not included in Second
        query = "( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'Haiqu' && BAR == 'BAR'";
        hitTermAssertions.withRequiredAllOf("UUID.0:Second", "NAME.2:Haiqu", "BAR.0:BAR");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testLargeNestedUnionStillOnlySecondDocumentReturned() throws Exception {
        // try to pull in hits from Third, should still hit only Second
        query = "( UUID == 'First' || UUID == 'Second' || UUID == 'Third') &&  NAME == 'Haiqu' && BAR == 'BAR'";
        hitTermAssertions.withRequiredAllOf("UUID.0:Second", "NAME.2:Haiqu", "BAR.0:BAR");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testNestedUnionWithFilterAnchorTerms() throws Exception {
        query = "( UUID == 'First' || UUID == 'Second' || UUID == 'Third') &&  filter:includeRegex(NAME,'Haiqu') && filter:includeRegex(BAR,'BAR')";
        hitTermAssertions.withRequiredAllOf("UUID.0:Second", "NAME.2:Haiqu", "BAR.0:BAR");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testSecondAndBarBar() throws Exception {
        query = "UUID == 'Second' && BAR == 'BAR'";
        hitTermAssertions.withRequiredAllOf("UUID.0:Second", "BAR.0:BAR");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testFilterOccurrenceFunction() throws Exception {
        query = "NAME == 'Haiqu' && BAR == 'BAR' && filter:occurrence(NAME, '==', 3)";
        hitTermAssertions.withRequiredAllOf("BAR.0:BAR", "NAME.2:Haiqu");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    @Test
    public void testFilterIsNotNull() throws Exception {
        query = "UUID == 'First' && filter:isNotNull(NAME)";
        hitTermAssertions.withRequiredAnyOf("UUID.0:First");
        expectedResultCount = 1;
        drive();

        extraParameters.put("return.fields", "*");
        drive();
    }

    private static class MoreTestData {

        private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();

        protected static final String datatype = "test";
        protected static final String date = "20130101";
        protected static final String shard = date + "_0";
        protected static final ColumnVisibility columnVisibility = new ColumnVisibility("A");
        protected static final Value emptyValue = new Value(new byte[0]);
        protected static final long timeStamp = 1356998400000L;

        /**
         */
        public static void writeItAll(AccumuloClient client, WhatKindaRange range) throws Exception {
            BatchWriter bw = null;
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
            Mutation mutation;

            String firstUID = UID.builder().newId("First".getBytes(), (Date) null).toString();
            String secondUID = UID.builder().newId("Second".getBytes(), (Date) null).toString();
            String thirdUID = UID.builder().newId("Third".getBytes(), (Date) null).toString();

            try {
                // write the shard table :
                bw = client.createBatchWriter(TableName.SHARD, bwConfig);
                mutation = new Mutation(shard);
                // NAME.0 gets NAME0 and NAME.1 gets NAME1
                mutation.put(datatype + "\u0000" + firstUID, "NAME.0" + "\u0000" + "NAME0", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + firstUID, "NAME.1" + "\u0000" + "NAME1", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + firstUID, "NAME.2" + "\u0000" + "Haiqu", columnVisibility, timeStamp, emptyValue);
                // FOO is only in First
                mutation.put(datatype + "\u0000" + firstUID, "FOO.0" + "\u0000" + "FOO", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + firstUID, "UUID.0" + "\u0000" + "First", columnVisibility, timeStamp, emptyValue);

                // this event has NAME1 and NAME0 in the opposite fields
                mutation.put(datatype + "\u0000" + secondUID, "NAME.0" + "\u0000" + "NAME1", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + secondUID, "NAME.1" + "\u0000" + "NAME0", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + secondUID, "NAME.2" + "\u0000" + "Haiqu", columnVisibility, timeStamp, emptyValue);
                // BAR is only in Second
                mutation.put(datatype + "\u0000" + secondUID, "BAR.0" + "\u0000" + "BAR", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + secondUID, "UUID.0" + "\u0000" + "Second", columnVisibility, timeStamp, emptyValue);

                mutation.put(datatype + "\u0000" + thirdUID, "NAME.0" + "\u0000" + "NAME9", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + thirdUID, "NAME.1" + "\u0000" + "NAME8", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + thirdUID, "NAME.2" + "\u0000" + "Haiqu", columnVisibility, timeStamp, emptyValue);
                // BAZ is only in Third
                mutation.put(datatype + "\u0000" + thirdUID, "BAZ.0" + "\u0000" + "BAZ", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + thirdUID, "UUID.0" + "\u0000" + "Third", columnVisibility, timeStamp, emptyValue);

                bw.addMutation(mutation);

            } finally {
                if (null != bw) {
                    bw.close();
                }
            }

            try {
                // write shard index table:
                bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("First"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Second"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Third"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);

            } finally {
                if (null != bw) {
                    bw.close();
                }
            }

            try {

                // write the reverse index table:
                bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("First")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("Second")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("Third")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);

            } finally {
                if (null != bw) {
                    bw.close();
                }
            }

            try {

                // write the field index table:
                bw = client.createBatchWriter(TableName.SHARD, bwConfig);
                mutation = new Mutation(shard);

                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("First") + "\u0000" + datatype + "\u0000" + firstUID, columnVisibility,
                                timeStamp, emptyValue);

                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Second") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility,
                                timeStamp, emptyValue);

                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Third") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility,
                                timeStamp, emptyValue);

                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }

            try {
                // write metadata table:
                bw = client.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);

                mutation = new Mutation("NAME");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility, timeStamp,
                                emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("FOO");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility, timeStamp,
                                emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("BAR");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility, timeStamp,
                                emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("BAZ");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility, timeStamp,
                                emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("UUID");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility, timeStamp,
                                emptyValue);
                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
        }
    }

    private static Value getValueForBuilderFor(String... in) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        for (String s : in) {
            builder.addUID(s);
        }
        builder.setCOUNT(in.length);
        builder.setIGNORE(false);
        return new Value(builder.build().toByteArray());
    }

    private static Value getValueForNuthinAndYourHitsForFree() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        // technically this value could be zero due to delete mutations, depending on the combiner
        // set on the table the key may or may not be deleted.
        builder.setCOUNT(50); // better not be zero!!!!
        builder.setIGNORE(true); // better be true!!!
        return new Value(builder.build().toByteArray());
    }
}
