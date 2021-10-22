package datawave.query.jexl.functions;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
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
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.metrics.MockStatusReporter;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
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
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datawave.query.QueryParameters.DATE_RANGE_TYPE;
import static datawave.webservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.webservice.query.QueryParameters.QUERY_END;
import static datawave.webservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.webservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_PERSISTENCE;
import static datawave.webservice.query.QueryParameters.QUERY_STRING;

@RunWith(Arquillian.class)
public class ContentFunctionQueryTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private static final int NUM_SHARDS = 241;
    private static final String DATA_TYPE_NAME = "test";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    private static final String PASSWORD = "";
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20020101 000000.000";
    
    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";
    
    private static final Configuration conf = new Configuration();
    private static final String TEST_DATA = "datawave/query/jexl/functions/ContentFunctionQueryExample.csv";
    
    @Inject
    @SpringBean(name = "EventQuery")
    ShardQueryLogic logic;
    
    private static InMemoryInstance instance;
    
    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
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
        record.setDate(formatter.parse(BEGIN_DATE).getTime());
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
        instance = new InMemoryInstance();
        Connector connector = instance.getConnector("root", PASSWORD);
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));
        
        writeKeyValues(connector, keyValues);
        
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString()));
    }
    
    public static void setupConfiguration(Configuration conf) {
        
        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + ".data.category.index", "ID, BODY");
        // conf.set(DATA_TYPE_NAME + ".model.table.name", METADATA_TABLE_NAME);
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
    
    private static void writeKeyValues(Connector connector, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = connector.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
        tops.create(TableName.DATE_INDEX);
        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName))
                tops.create(tableName);
            
            final BatchWriter writer = connector.createBatchWriter(tableName, new BatchWriterConfig());
            for (final Value val : keyValues.get(biKey)) {
                final Mutation mutation = new Mutation(biKey.getKey().getRow());
                mutation.put(biKey.getKey().getColumnFamily(), biKey.getKey().getColumnQualifier(), biKey.getKey().getColumnVisibilityParsed(), biKey.getKey()
                                .getTimestamp(), val);
                writer.addMutation(mutation);
            }
            writer.close();
        }
    }
    
    @Test
    public void withinTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:within(1,termOffsetMap,'dog','cat')";
        
        final List<String> expected = Arrays.asList("dog", "cat");
        final List<DefaultEvent> events = getQueryResults(query, true, null);
        Assert.assertEquals(1, events.size());
        evaluateEvents(events, expected);
    }
    
    @Test
    public void withinTestWithAlternateDate() throws Exception {
        String query = "ID == 'TEST_ID' && content:within(1,termOffsetMap,'dog','cat')";
        
        MultiValueMap<String,String> optionalParams = new LinkedMultiValueMap<>();
        optionalParams.set(DATE_RANGE_TYPE, "BOGUSDATETYPE");
        
        final List<DefaultEvent> events = getQueryResults(query, true, optionalParams);
        Assert.assertEquals(0, events.size());
    }
    
    @Test
    public void withinSkipTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:within(1,termOffsetMap,'dog','boy')";
        
        final List<DefaultEvent> events = getQueryResults(query, true, null);
        Assert.assertEquals(1, events.size());
        final List<String> expected = Arrays.asList("dog", "boy");
        evaluateEvents(events, expected);
    }
    
    @Test
    public void phraseTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:phrase(termOffsetMap,'boy','car')";
        
        final List<DefaultEvent> events = getQueryResults(query, true, null);
        Assert.assertEquals(1, events.size());
        final List<String> expected = Arrays.asList("boy", "car");
        evaluateEvents(events, expected);
    }
    
    @Test
    public void phraseWithSkipTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:phrase(termOffsetMap,'dog','gap')";
        
        final List<DefaultEvent> events = getQueryResults(query, true, null);
        Assert.assertEquals(1, events.size());
        final List<String> expected = Arrays.asList("dog", "gap");
        evaluateEvents(events, expected);
    }
    
    @Test
    public void phraseScoreTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:scoredPhrase(-1.5, termOffsetMap,'boy','car')";
        
        final List<DefaultEvent> events = getQueryResults(query, true, null);
        Assert.assertEquals(1, events.size());
        final List<String> expected = Arrays.asList("boy", "car");
        evaluateEvents(events, expected);
    }
    
    @Test
    public void phraseScoreFilterTest() throws Exception {
        String query = "ID == 'TEST_ID' && content:scoredPhrase(-1.4, termOffsetMap,'boy','car')";
        
        final List<DefaultEvent> events = getQueryResults(query, true, null);
        Assert.assertEquals(0, events.size());
        Assert.assertEquals("Expected no results", 0, events.size());
    }
    
    private static void evaluateEvents(List<DefaultEvent> events, List<String> expected) {
        
        Assert.assertTrue("Expected 1 or more results", events.size() >= 1);
        
        for (DefaultEvent event : events) {
            
            List<String> fields = event.getFields().stream().filter((DefaultField field) -> expected.contains(field.getValueString()))
                            .map(DefaultField::getValueString).distinct().collect(Collectors.toList());
            
            Assert.assertTrue("Missing values {" + expected + "} != {" + fields + "}", fields.containsAll(expected));
        }
    }
    
    private List<DefaultEvent> getQueryResults(String queryString, boolean useIvarator, MultiValueMap<String,String> optionalParams) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic(useIvarator);
        
        Iterator iter = getResultsIterator(queryString, logic, optionalParams);
        List<DefaultEvent> events = new ArrayList<>();
        while (iter.hasNext())
            events.add((DefaultEvent) iter.next());
        return events;
    }
    
    private Iterator getResultsIterator(String queryString, ShardQueryLogic logic, MultiValueMap<String,String> optionalParams) throws Exception {
        MultiValueMap<String,String> params = new LinkedMultiValueMap<>();
        params.set(QUERY_STRING, queryString);
        params.set(QUERY_NAME, "contentQuery");
        params.set(QUERY_LOGIC_NAME, "EventQueryLogic");
        params.set(QUERY_PERSISTENCE, "PERSISTENT");
        params.set(QUERY_AUTHORIZATIONS, AUTHS);
        params.set(QUERY_EXPIRATION, "20200101 000000.000");
        params.set(QUERY_BEGIN, BEGIN_DATE);
        params.set(QUERY_END, END_DATE);
        
        QueryParameters queryParams = new QueryParametersImpl();
        queryParams.validate(params);
        
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations(AUTHS));
        
        Query query = new QueryImpl();
        query.initialize(USER, Arrays.asList(USER_DN), null, queryParams, optionalParams);
        
        ShardQueryConfiguration config = ShardQueryConfiguration.create(logic, query);
        
        logic.initialize(config, instance.getConnector("root", PASSWORD), query, auths);
        
        logic.setupQuery(config);
        
        return logic.getTransformIterator(query);
    }
    
    private ShardQueryLogic getShardQueryLogic(boolean useIvarator) {
        ShardQueryLogic logic = new ShardQueryLogic(this.logic);
        
        // increase the depth threshold
        logic.setMaxDepthThreshold(20);
        
        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setPushdownThreshold(1000000);
        
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());
        logic.setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);
        
        if (useIvarator)
            setupIvarator(logic);
        
        return logic;
    }
    
    private void setupIvarator(ShardQueryLogic logic) {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        logic.setMaxOrExpansionFstThreshold(1);
        logic.setIvaratorCacheScanPersistThreshold(1);
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
            content.lines().forEach(
                            line -> {
                                String[] parts = line.split(",");
                                TermWeight.Info info = TermWeight.Info.newBuilder().addTermOffset(Integer.parseInt(parts[0]))
                                                .addPrevSkips(Integer.parseInt(parts[1]))
                                                .addScore(TermWeightPosition.positionScoreToTermWeightScore(Float.parseFloat(parts[3])))
                                                .setZeroOffsetMatch(true).build();
                                
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
            Multimap<BulkIngestKey,Value> termIndex = createTermIndexColumn(event, nfv.getEventFieldName(), nfv.getEventFieldValue(),
                            getVisibility(event, nfv), null, null, shardId, this.getShardIndexTableName(), new Value(uid.toByteArray()));
            values.putAll(termIndex);
        }
        
        private void getTFKey(final NormalizedFieldAndValue nfv, final RawRecordContainer event, final Multimap values, final TermWeight.Info info) {
            byte[] fieldVisibility = getVisibility(event, nfv);
            StringBuilder colq = new StringBuilder(this.eventDataTypeName.length() + this.eventUid.length() + nfv.getIndexedFieldName().length()
                            + nfv.getIndexedFieldValue().length() + 3);
            colq.append(this.eventDataTypeName).append('\u0000').append(this.eventUid).append('\u0000').append(nfv.getIndexedFieldValue()).append('\u0000')
                            .append(nfv.getIndexedFieldName());
            
            BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), new Key(shardId,
                            ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY.getBytes(), colq.toString().getBytes(), fieldVisibility, event.getDate(),
                            helper.getDeleteMode()));
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
