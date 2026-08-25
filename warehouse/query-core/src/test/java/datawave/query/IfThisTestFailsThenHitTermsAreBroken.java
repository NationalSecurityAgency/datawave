package datawave.query;

import java.nio.file.Path;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.table.constants.MetadataColumnFamilyConstants;
import datawave.table.constants.TableName;

/**
 *
 * This test confirms that hit terms are found in the correct documents, and only in the correct documents. The test data has fields that will hit in different
 * grouping context levels, and assures that the hits contain the fields with the correct grouping context. It also confirms that an 'or' query that hits
 * different fields in the returned documents will have the correct hit terms.
 * <p>
 * If this test fails, then hit terms are broken... maybe... probably...
 */
public class IfThisTestFailsThenHitTermsAreBroken extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(IfThisTestFailsThenHitTermsAreBroken.class);
    private static final Authorizations auths = new Authorizations("A");

    private static AccumuloClient clientForTest;

    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        disableQueryPlanAssertion();
    }

    @Override
    protected void extraAssertions() {
        // no-op; hit term validation is handled by AbstractQueryTest via expectHitTermsRequiredAllOf/AnyOf
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(IfThisTestFailsThenHitTermsAreBroken.class.toString(), log);
        clientForTest = qtth.client;
        MoreTestData.writeItAll(clientForTest);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup(@TempDir Path tempDir) throws Exception {
        System.setProperty("type.metadata.dir", tempDir.toAbsolutePath().toString());
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D,T,U,V,W,X,Y,Z");

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

        setClientForTest(clientForTest);
        givenDate("20091231", "20150101");
        givenParameter("hit.list", "true");
    }

    @AfterEach
    public void tearDown() {
        System.clearProperty("type.metadata.dir");
    }

    @Test
    public void testUnion() throws Exception {
        // sanity check that expects two documents
        givenQuery("UUID == 'First' || UUID == 'Second'");
        expectHitTermsRequiredAnyOf("UUID.0:First", "UUID.0:Second");
        expectResultCount(2);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testDoubleNestedUnion() throws Exception {
        // look for FOO or BAR, expecting the hit_terms to be in the right places
        givenQuery("( UUID == 'First' || UUID == 'Second' ) && ( FOO == 'FOO' || BAR == 'BAR' )");
        expectHitTermsRequiredAnyOf("UUID.0:First", "UUID.0:Second");
        expectHitTermsRequiredAnyOf("FOO.0:FOO", "BAR.0:BAR");
        expectResultCount(2);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testNestedUnionWithAnchorOfDifferentGroupingContext() throws Exception {
        // should find NAME0 in different grouping contexts, but the hit terms should be correct
        givenQuery("( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'NAME0'");
        expectHitTermsRequiredAnyOf("UUID.0:First", "UUID.0:Second");
        expectHitTermsRequiredAnyOf("NAME.0:NAME0", "NAME.1:NAME0");
        expectResultCount(2);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testNestedUnionFirstDocumentReturned() throws Exception {
        // this may get initial hits in Second, but will return only First. Makes sure that hits from Second are not included in First
        givenQuery("( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'Haiqu' && FOO == 'FOO'");
        expectHitTermsRequiredAllOf("UUID.0:First", "NAME.2:Haiqu", "FOO.0:FOO");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testNestedUnionSecondDocumentReturned() throws Exception {
        // this may get initial hits in First, but will return only Second. Makes sure that hits from First are not included in Second
        givenQuery("( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'Haiqu' && BAR == 'BAR'");
        expectHitTermsRequiredAllOf("UUID.0:Second", "NAME.2:Haiqu", "BAR.0:BAR");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testLargeNestedUnionStillOnlySecondDocumentReturned() throws Exception {
        // try to pull in hits from Third, should still hit only Second
        givenQuery("( UUID == 'First' || UUID == 'Second' || UUID == 'Third') &&  NAME == 'Haiqu' && BAR == 'BAR'");
        expectHitTermsRequiredAllOf("UUID.0:Second", "NAME.2:Haiqu", "BAR.0:BAR");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testNestedUnionWithFilterAnchorTerms() throws Exception {
        givenQuery("( UUID == 'First' || UUID == 'Second' || UUID == 'Third') &&  filter:includeRegex(NAME,'Haiqu') && filter:includeRegex(BAR,'BAR')");
        expectHitTermsRequiredAllOf("UUID.0:Second", "NAME.2:Haiqu", "BAR.0:BAR");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testSecondAndBarBar() throws Exception {
        givenQuery("UUID == 'Second' && BAR == 'BAR'");
        expectHitTermsRequiredAllOf("UUID.0:Second", "BAR.0:BAR");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testFilterOccurrenceFunction() throws Exception {
        givenQuery("NAME == 'Haiqu' && BAR == 'BAR' && filter:occurrence(NAME, '==', 3)");
        expectHitTermsRequiredAllOf("BAR.0:BAR", "NAME.2:Haiqu");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    @Test
    public void testFilterIsNotNull() throws Exception {
        givenQuery("UUID == 'First' && filter:isNotNull(NAME)");
        expectHitTermsRequiredAnyOf("UUID.0:First");
        expectResultCount(1);
        planAndExecuteQuery();

        givenParameter("return.fields", "*");
        planAndExecuteQuery();
    }

    private static class MoreTestData {

        private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
        private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

        protected static final String datatype = "test";
        protected static final String date = "20130101";
        protected static final String shard = date + "_0";
        protected static final ColumnVisibility columnVisibility = new ColumnVisibility("A");
        protected static final Value emptyValue = new Value(new byte[0]);
        protected static final long timeStamp = 1356998400000L;

        /**
         * Writes test data using the document range only; {@link IndexIngestUtil} derives the other shard index table variants (NO_UID_INDEX, TRUNCATED_INDEX,
         * etc.) that {@link AbstractQueryTest} iterates over.
         */
        public static void writeItAll(AccumuloClient client) throws Exception {
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
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Second"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Third"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, getValueForBuilderFor(thirdUID));
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
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("Second")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);

                bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("Third")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, getValueForBuilderFor(thirdUID));
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
                mutation.put(MetadataColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("FOO");
                mutation.put(MetadataColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("BAR");
                mutation.put(MetadataColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("BAZ");
                mutation.put(MetadataColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("UUID");
                mutation.put(MetadataColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_I, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_RI, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(MetadataColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }

            Authorizations auths = new Authorizations("A");
            ingestUtil.write(client, auths);
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
}
