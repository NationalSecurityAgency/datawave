package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.microservice.query.QueryImpl;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.model.Direction;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.testframework.MockStatusReporter;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.ColumnFamilyConstants;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ContentFunctionQueryTest extends AbstractQueryTest {

    private static final int NUM_SHARDS = 241;
    private static final String DATA_TYPE_NAME = "test";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();

    private static final String AUTHS = "ALL";
    private static final Authorizations auths = new Authorizations(AUTHS);

    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);

    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20020101 000000.000";

    private static final Configuration conf = new Configuration();
    private static final String TEST_DATA = "datawave/query/jexl/functions/ContentFunctionQueryExample.csv";

    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient clientForTest;
    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;

    private ShardQueryLogic currentLogic;
    private List<DefaultEvent> events = new ArrayList<>();
    private int expectedEventCount = -1;
    private List<String> expectedValues = new ArrayList<>();

    @Override
    public ShardQueryLogic getLogic() {
        return currentLogic;
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
    protected QueryImpl getSettings() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(formatter.parse(BEGIN_DATE));
        settings.setEndDate(formatter.parse(END_DATE));
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(getAuths().serialize());
        settings.setQuery(getQuery());
        settings.setParameters(getParameters());
        settings.setId(java.util.UUID.randomUUID());
        return settings;
    }

    @Override
    protected void executeQuery(ShardQueryLogic logic) throws Exception {
        try {
            Iterator<?> iter = logic.getTransformIterator(logic.getConfig().getQuery());
            events = new ArrayList<>();
            while (iter.hasNext()) {
                events.add((DefaultEvent) iter.next());
            }
        } finally {
            logic.close();
        }
    }

    @Override
    protected void extraAssertions() {
        assertEquals(expectedEventCount, events.size());
        if (!expectedValues.isEmpty()) {
            evaluateEvents(events, expectedValues);
        }
    }

    @BeforeAll
    public static void setupClass(@TempDir Path tempDir) throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");

        byte[] data = IOUtils.toByteArray(ClassLoader.getSystemResource(TEST_DATA).openStream());

        setupConfiguration(conf);

        TestContentIndexingHandler dataTypeHandler = new TestContentIndexingHandler();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);

        // create and process events with test data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();

        record.clear();
        record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, null, null, 1, null));
        record.setRawFileName("example.dat");
        record.setRawRecordNumber(1);
        record.setTimestamp(formatter.parse(BEGIN_DATE).getTime());
        record.setRawData(data);
        record.generateId(null);
        record.setVisibility(new ColumnVisibility(AUTHS));

        final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);

        MockStatusReporter statusReporter = new MockStatusReporter();
        Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, statusReporter);
        Multimap content = dataTypeHandler.processContent(record, fields, statusReporter);

        keyValues.putAll(kvPairs);
        keyValues.putAll(content);

        fields.put("BODY", new NormalizedFieldAndValue("BODY", "_"));
        dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());

        // write these values to their respective tables
        InMemoryInstance instance = new InMemoryInstance();

        AccumuloClient client = new InMemoryAccumuloClient("root", instance);
        client.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));

        writeKeyValues(client, keyValues);
        clientForTest = client;

        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(tempDir.toUri().toString()));
    }

    public static void setupConfiguration(Configuration conf) {

        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + ".data.category.index", "ID, BODY");
        conf.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, INGEST_HELPER_CLASS);
        conf.set(DateIndexDataTypeHandler.DATEINDEX_TNAME, TableName.DATE_INDEX);
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, TableName.METADATA);
        conf.set(ShardedDataTypeHandler.NUM_SHARDS, Integer.toString(NUM_SHARDS));
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TableName.SHARD + "," + TableName.ERROR_SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_LPRIORITY, "30");
        conf.set(TableName.SHARD + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_LPRIORITY, "30");
        conf.set(TableName.SHARD_INDEX + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_LPRIORITY, "30");
        conf.set(TableName.SHARD_RINDEX + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());

    }

    private static void writeKeyValues(AccumuloClient client, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = client.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
        tops.create(TableName.DATE_INDEX);
        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName))
                tops.create(tableName);

            final BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig());
            for (final Value val : keyValues.get(biKey)) {
                final Mutation mutation = new Mutation(biKey.getKey().getRow());
                mutation.put(biKey.getKey().getColumnFamily(), biKey.getKey().getColumnQualifier(), biKey.getKey().getColumnVisibilityParsed(),
                                biKey.getKey().getTimestamp(), val);
                writer.addMutation(mutation);
            }
            writer.close();
        }

        // write with the document range only; IndexIngestUtil derives the other shard index table variants
        // that AbstractQueryTest iterates over.
        ingestUtil.write(client, new Authorizations(AUTHS));
    }

    private void setupLogic(boolean useIvarator) {
        setClientForTest(clientForTest);

        currentLogic = new ShardQueryLogic(this.logic);

        // increase the depth threshold
        currentLogic.setMaxDepthThreshold(20);

        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (currentLogic.getQueryPlanner())).setPushdownThreshold(1000000);

        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        currentLogic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());
        currentLogic.setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);

        if (useIvarator)
            setupIvarator(currentLogic);
    }

    private void setupIvarator(ShardQueryLogic logic) {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        logic.setMaxOrExpansionFstThreshold(1);
        logic.setIvaratorCacheScanPersistThreshold(1);
    }

    private void runQuery(String query, boolean useIvarator, int expectedCount, List<String> expected) throws Exception {
        setupLogic(useIvarator);
        this.expectedEventCount = expectedCount;
        this.expectedValues = expected;
        givenQuery(query);
        planAndExecuteQuery();
    }

    @Test
    public void withinTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:within(1,termOffsetMap,'dog','cat')";
        runQuery(query, true, 1, Arrays.asList("dog", "cat"));
    }

    @Test
    public void withinTestWithAlternateDate() throws Exception {
        String query = "ID == 'TEST_ID' && content:within(1,termOffsetMap,'dog','cat')";

        setupLogic(true);
        this.expectedEventCount = 0;
        this.expectedValues = Collections.emptyList();
        givenQuery(query);
        givenParameter(datawave.query.QueryParameters.DATE_RANGE_TYPE, "BOGUSDATETYPE");
        planAndExecuteQuery();
    }

    @Test
    public void withinSkipTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:within(1,termOffsetMap,'dog','boy')";
        runQuery(query, true, 1, Arrays.asList("dog", "boy"));
    }

    @Test
    public void phraseTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:phrase(termOffsetMap,'boy','car')";
        runQuery(query, true, 1, Arrays.asList("boy", "car"));
    }

    @Test
    public void phraseWithSkipTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:phrase(termOffsetMap,'dog','gap')";
        runQuery(query, true, 1, Arrays.asList("dog", "gap"));
    }

    @Test
    public void phraseScoreTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:scoredPhrase(-1.5, termOffsetMap,'boy','car')";
        runQuery(query, true, 1, Arrays.asList("boy", "car"));
    }

    @Test
    public void phraseScoreFilterTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:scoredPhrase(-1.4, termOffsetMap,'boy','car')";
        runQuery(query, true, 0, Collections.emptyList());
    }

    private static void evaluateEvents(List<DefaultEvent> events, List<String> expected) {

        assertTrue(events.size() >= 1, "Expected 1 or more results");

        for (DefaultEvent event : events) {

            List<String> fields = event.getFields().stream().filter((DefaultField field) -> expected.contains(field.getValueString()))
                            .map(DefaultField::getValueString).distinct().collect(Collectors.toList());

            assertTrue(fields.containsAll(expected), "Missing values {" + expected + "} != {" + fields + "}");
        }
    }

    public static class TestIngestHelper extends ContentBaseIngestHelper implements TermFrequencyIngestHelperInterface {

        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
            Multimap<String,NormalizedContentInterface> events = HashMultimap.create();
            NormalizedContentInterface id = new NormalizedFieldAndValue("ID", "TEST_ID");
            events.put("ID", id);

            return normalizeMap(events);
        }

        @Override
        public boolean isTermFrequencyField(String fieldName) {
            return "BODY".equalsIgnoreCase(fieldName);
        }
    }

    public static class TestContentIndexingHandler extends ContentIndexingColumnBasedHandler<Text> {

        public Multimap<BulkIngestKey,Value> processContent(final RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                        StatusReporter reporter) {

            BufferedReader content = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(event.getRawData())));

            Multimap<BulkIngestKey,Value> values = HashMultimap.create();

            // Process test file
            // CSV file => position,skips,term,score\n
            content.lines().forEach(line -> {
                String[] parts = line.split(",");
                TermWeight.Info info = TermWeight.Info.newBuilder().addTermOffset(Integer.parseInt(parts[0])).addPrevSkips(Integer.parseInt(parts[1]))
                                .addScore(TermWeightPosition.positionScoreToTermWeightScore(Float.parseFloat(parts[3]))).setZeroOffsetMatch(true).build();

                NormalizedFieldAndValue nfv = new NormalizedFieldAndValue("BODY", parts[2]);
                getShardFIKey(nfv, event, values);
                getShardIndexFIKey(nfv, event, values);
                getTFKey(nfv, event, values, info);

            });

            return values;
        }

        private void getShardFIKey(final NormalizedFieldAndValue nfv, final RawRecordContainer event, final Multimap values) {
            createShardFieldIndexColumn(event, values, nfv.getEventFieldName(), nfv.getEventFieldValue(), getVisibility(event, nfv), shardId, this.eventUid,
                            event.getDate(), null);
        }

        private void getShardIndexFIKey(final NormalizedFieldAndValue nfv, final RawRecordContainer event, final Multimap values) {
            Uid.List uid = Uid.List.newBuilder().setIGNORE(false).setCOUNT(1).addUID(this.eventUid).build();
            Multimap<BulkIngestKey,Value> termIndex = createTermIndexColumn(event, nfv.getEventFieldName(), nfv.getEventFieldValue(), getVisibility(event, nfv),
                            null, null, shardId, this.getShardIndexTableName(), new Value(uid.toByteArray()), Direction.FORWARD);
            values.putAll(termIndex);
        }

        private void getTFKey(final NormalizedFieldAndValue nfv, final RawRecordContainer event, final Multimap values, final TermWeight.Info info) {
            byte[] fieldVisibility = getVisibility(event, nfv);
            StringBuilder colq = new StringBuilder(this.eventDataTypeName.length() + this.eventUid.length() + nfv.getIndexedFieldName().length()
                            + nfv.getIndexedFieldValue().length() + 3);
            colq.append(this.eventDataTypeName).append('\u0000').append(this.eventUid).append('\u0000').append(nfv.getIndexedFieldValue()).append('\u0000')
                            .append(nfv.getIndexedFieldName());

            BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), new Key(shardId, ColumnFamilyConstants.TERM_FREQUENCY_TEXT.getBytes(),
                            colq.toString().getBytes(), fieldVisibility, event.getDate(), helper.getDeleteMode()));
            values.put(bKey, new Value(info.toByteArray()));
        }

        @Override
        public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
            return new TestContentIndexingHelper();
        }
    }

    public static class TestContentIndexingHelper extends AbstractContentIngestHelper implements TermFrequencyIngestHelperInterface {
        @Override
        public String getTokenFieldNameDesignator() {
            return "";
        }

        @Override
        public boolean isReverseContentIndexField(String field) {
            return false;
        }

        @Override
        public boolean getSaveRawDataOption() {
            return false;
        }

        @Override
        public String getRawDocumentViewName() {
            return null;
        }

        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
            Multimap<String,NormalizedContentInterface> events = HashMultimap.create();
            NormalizedContentInterface id = new NormalizedFieldAndValue("ID", "TEST_ID");
            events.put("ID", id);

            return normalizeMap(events);
        }

        @Override
        public boolean isContentIndexField(String field) {
            return "BODY".equalsIgnoreCase(field);
        }

        @Override
        public boolean isTermFrequencyField(String field) {
            return "BODY".equalsIgnoreCase(field);
        }

        @Override
        public boolean isIndexListField(String field) {
            return false;
        }

        @Override
        public boolean isReverseIndexListField(String field) {
            return false;
        }

        @Override
        public String getListDelimiter() {
            return ",";
        }

    }
}
