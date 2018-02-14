package datawave.query.iterator.filter.field.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.FieldIndexData;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.config.ShardQueryConfigurationFactory;
import datawave.query.iterator.QueryIterator;
import datawave.query.jexl.StatefulArithmetic;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.metrics.MockStatusReporter;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;
import datawave.webservice.edgedictionary.TestDatawaveEdgeDictionaryImpl;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import io.protostuff.ProtobufIOUtil;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static datawave.webservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.webservice.query.QueryParameters.QUERY_END;
import static datawave.webservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.webservice.query.QueryParameters.QUERY_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_PERSISTENCE;
import static datawave.webservice.query.QueryParameters.QUERY_STRING;

@RunWith(Arquillian.class)
public class FieldIndexFilterQueryTest {
    private static final int NUM_SHARDS = 241;
    private static final String SHARD_TABLE_NAME = "shard";
    private static final String KNOWLEDGE_SHARD_TABLE_NAME = "knowledgeShard";
    private static final String ERROR_SHARD_TABLE_NAME = "errorShard";
    private static final String SHARD_INDEX_TABLE_NAME = "shardIndex";
    private static final String SHARD_REVERSE_INDEX_TABLE_NAME = "shardReverseIndex";
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    private static final String FIF_ENABLED_FIELD_NAME = "FIF_TEXT_FIELD";
    private static final String FIF_DISABLED_FIELD_NAME = "TEXT_FIELD";
    private static final String MAPPED_FIELD = "MAPPED_FIELD";
    private static final String UNMAPPED_FIELD = "UNMAPPED_FIELD";
    
    private static final String PASSWORD = "";
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20000102 000000.000";
    
    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";
    
    private static final Configuration conf = new Configuration();
    
    // @formatter:off
    private static final String[] stringData = {
            "never gonna give you up",
            "never gonna let you down",
            "never gonna run around and desert you",
            "never gonna make you cry",
            "never gonna say goodbye",
            "never gonna tell a lie and hurt you"
    };
    // @formatter:on
    
    @Inject
    @SpringBean(name = "EventQuery")
    ShardQueryLogic logic;
    
    private static InMemoryInstance instance;
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "datawave.webservice.query.result.event")
                        .addClass(TestDatawaveEdgeDictionaryImpl.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");
        
        setupConfiguration(conf);
        
        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<Text>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);
        
        // create and process events with string data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;
        for (int i = 0; i < stringData.length; i++) {
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("stringdata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setDate(formatter.parse(BEGIN_DATE).getTime());
            record.setRawDataAndGenerateId(stringData[i].getBytes("UTF8"));
            record.setVisibility(new ColumnVisibility(AUTHS));
            
            final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);
            
            Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());
            
            keyValues.putAll(kvPairs);
            
            dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
        }
        
        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());
        
        // write these values to their respective tables
        instance = new InMemoryInstance();
        Connector connector = instance.getConnector("root", PASSWORD);
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));
        
        writeKeyValues(connector, keyValues);
    }
    
    public static void setupConfiguration(Configuration conf) {
        conf.set(ShardedDataTypeHandler.SHARD_FIELD_INDEX_FILTER_ENABLED, "true");
        conf.set(DATA_TYPE_NAME + "." + FIF_ENABLED_FIELD_NAME + BaseIngestHelper.FIELD_INDEX_FILTER_MAPPING, MAPPED_FIELD);
        
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, FIF_ENABLED_FIELD_NAME + "," + FIF_DISABLED_FIELD_NAME);
        conf.set(DATA_TYPE_NAME + "." + FIF_ENABLED_FIELD_NAME + BaseIngestHelper.FIELD_TYPE, NoOpType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + FIF_DISABLED_FIELD_NAME + BaseIngestHelper.FIELD_TYPE, NoOpType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + MAPPED_FIELD + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + UNMAPPED_FIELD + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        
        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        conf.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, INGEST_HELPER_CLASS);
        
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, METADATA_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.NUM_SHARDS, Integer.toString(NUM_SHARDS));
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, SHARD_TABLE_NAME + "," + KNOWLEDGE_SHARD_TABLE_NAME + "," + ERROR_SHARD_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, SHARD_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_LPRIORITY, "30");
        conf.set(SHARD_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, SHARD_INDEX_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_LPRIORITY, "30");
        conf.set(SHARD_INDEX_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, SHARD_REVERSE_INDEX_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_LPRIORITY, "30");
        conf.set(SHARD_REVERSE_INDEX_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_ENABLED, "false");
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_CONFIG, "");
        conf.set("partitioner.category.shardedTables", BalancedShardPartitioner.class.getName());
        conf.set("partitioner.category.member." + SHARD_TABLE_NAME, "shardedTables");
    }
    
    private static void writeKeyValues(Connector connector, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = connector.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
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
    
    // Run tests with and without ivarator enabled
    // ## query against FIF_ENABLED_FIELD_NAME, filtered against MAPPED_FIELD, with FIF ENABLED -> verify nonempty accepted and rejected
    @Test
    public void testFieldIndexFilterEnabled() throws Exception {
        internalTest(FIF_ENABLED_FIELD_NAME, MAPPED_FIELD, false, true, true);
        internalTest(FIF_ENABLED_FIELD_NAME, MAPPED_FIELD, true, true, true);
    }
    
    // ## query against FIF_ENABLED_FIELD_NAME, filtered against MAPPED_FIELD, with FIF DISABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterDisabled() throws Exception {
        internalTest(FIF_ENABLED_FIELD_NAME, MAPPED_FIELD, false, false, false);
        internalTest(FIF_ENABLED_FIELD_NAME, MAPPED_FIELD, true, false, false);
    }
    
    // ## query against FIF_ENABLED_FIELD_NAME, filtered against UNMAPPED_FIELD, with FIF ENABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterEnabledWithUnmappedField() throws Exception {
        internalTest(FIF_ENABLED_FIELD_NAME, UNMAPPED_FIELD, false, true, false);
        internalTest(FIF_ENABLED_FIELD_NAME, UNMAPPED_FIELD, true, true, false);
    }
    
    // ## query against FIF_ENABLED_FIELD_NAME, filtered against UNMAPPED_FIELD, with FIF DISABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterDisabledWithUnmappedField() throws Exception {
        internalTest(FIF_ENABLED_FIELD_NAME, UNMAPPED_FIELD, false, false, false);
        internalTest(FIF_ENABLED_FIELD_NAME, UNMAPPED_FIELD, true, false, false);
    }
    
    // ## query against FIF_DISABLED_FIELD_NAME, filtered against MAPPED_FIELD, with FIF ENABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterEnabledOnDisabledField() throws Exception {
        internalTest(FIF_DISABLED_FIELD_NAME, MAPPED_FIELD, false, true, false);
        internalTest(FIF_DISABLED_FIELD_NAME, MAPPED_FIELD, true, true, false);
    }
    
    // ## query against FIF_DISABLED_FIELD_NAME, filtered against MAPPED_FIELD, with FIF DISABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterDisabledOnDisabledField() throws Exception {
        internalTest(FIF_DISABLED_FIELD_NAME, MAPPED_FIELD, false, false, false);
        internalTest(FIF_DISABLED_FIELD_NAME, MAPPED_FIELD, true, false, false);
    }
    
    // ## query against FIF_DISABLED_FIELD_NAME, filtered against UNMAPPED_FIELD, with FIF ENABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterEnabledWithUnmappedFieldOnDisabledField() throws Exception {
        internalTest(FIF_DISABLED_FIELD_NAME, UNMAPPED_FIELD, false, true, false);
        internalTest(FIF_DISABLED_FIELD_NAME, UNMAPPED_FIELD, true, true, false);
    }
    
    // ## query against FIF_DISABLED_FIELD_NAME, filtered against UNMAPPED_FIELD, with FIF DISABLED -> verify empty accepted and rejected
    @Test
    public void testFieldIndexFilterDisabledWithUnmappedFieldOnDisabledField() throws Exception {
        internalTest(FIF_DISABLED_FIELD_NAME, UNMAPPED_FIELD, false, false, false);
        internalTest(FIF_DISABLED_FIELD_NAME, UNMAPPED_FIELD, true, false, false);
    }
    
    private void internalTest(String queryField, String filterField, boolean useIvarator, boolean useFieldIndexFilter, boolean expectFilteredFields)
                    throws Exception {
        String query = queryField + " >= 'never gonna r' && " + queryField + " <= 'never gonna z' && " + filterField + " < 25";
        
        List<DefaultEvent> events = getQueryResults(query, useIvarator, useFieldIndexFilter);
        
        // verify query results
        Assert.assertEquals(1, events.size());
        for (DefaultEvent event : events) {
            String stringFieldValue = null;
            Integer intFieldValue = null;
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(queryField))
                    stringFieldValue = field.getValueString();
                else if (field.getName().equals(filterField))
                    intFieldValue = Integer.parseInt(field.getValueString());
            }
            Assert.assertTrue(stringFieldValue.compareTo("never gonna r") >= 0 && stringFieldValue.compareTo("never gonna z") <= 0);
            Assert.assertTrue(intFieldValue < 25);
        }
        
        if (expectFilteredFields) {
            // verify accepted
            Assert.assertEquals(1, TestFieldIndexFilter.acceptedFields.size());
            for (Tuple3<String,String,Value> field : TestFieldIndexFilter.acceptedFields) {
                String dataType = field.first();
                String fieldName = field.second();
                FieldIndexData fieldIndexData = new FieldIndexData();
                ProtobufIOUtil.mergeFrom(field.third().get(), fieldIndexData, FieldIndexData.SCHEMA);
                
                Assert.assertEquals(DATA_TYPE_NAME, dataType);
                Assert.assertEquals(queryField, fieldName);
                Assert.assertEquals(1, fieldIndexData.getFilterData().getFieldValueMapping().size());
                Assert.assertEquals(1, fieldIndexData.getFilterData().getFieldValueMapping().get(filterField).size());
                Assert.assertTrue(Integer.parseInt(fieldIndexData.getFilterData().getFieldValueMapping().get(filterField).iterator().next()) < 25);
            }
            
            // verify rejected
            Assert.assertEquals(2, TestFieldIndexFilter.rejectedFields.size());
            for (Tuple3<String,String,Value> field : TestFieldIndexFilter.rejectedFields) {
                String dataType = field.first();
                String fieldName = field.second();
                FieldIndexData fieldIndexData = new FieldIndexData();
                ProtobufIOUtil.mergeFrom(field.third().get(), fieldIndexData, FieldIndexData.SCHEMA);
                
                Assert.assertEquals(DATA_TYPE_NAME, dataType);
                Assert.assertEquals(queryField, fieldName);
                Assert.assertEquals(1, fieldIndexData.getFilterData().getFieldValueMapping().size());
                Assert.assertEquals(1, fieldIndexData.getFilterData().getFieldValueMapping().get(filterField).size());
                Assert.assertFalse(Integer.parseInt(fieldIndexData.getFilterData().getFieldValueMapping().get(filterField).iterator().next()) < 25);
            }
        } else {
            // verify accepted
            Assert.assertEquals(0, TestFieldIndexFilter.acceptedFields.size());
            
            // verify rejected
            Assert.assertEquals(0, TestFieldIndexFilter.rejectedFields.size());
        }
    }
    
    private List<DefaultEvent> getQueryResults(String queryString, boolean useIvarator, boolean useFieldIndexFilter) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic();
        
        logic.setFieldIndexFilterEnabled(useFieldIndexFilter);
        
        if (useIvarator)
            setupIvarator(logic);
        
        TestFieldIndexFilter.acceptedFields.clear();
        TestFieldIndexFilter.rejectedFields.clear();
        
        Iterator iter = getResultsIterator(queryString, logic);
        List<DefaultEvent> events = new ArrayList<>();
        while (iter.hasNext())
            events.add((DefaultEvent) iter.next());
        return events;
    }
    
    private ShardQueryLogic getShardQueryLogic() {
        ShardQueryLogic logic = new ShardQueryLogic(this.logic);
        
        // increase the depth threshold
        logic.setMaxDepthThreshold(10);
        
        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setPushdownThreshold(1000000);
        
        // lets avoid condensing uids to ensure that shard ranges are not collapsed into day ranges
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setCondenseUidsInRangeStream(false);
        
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());
        
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
    
    private Iterator getResultsIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "testQuery");
        params.putSingle(QUERY_PERSISTENCE, "PERSISTENT");
        params.putSingle(QUERY_AUTHORIZATIONS, AUTHS);
        params.putSingle(QUERY_EXPIRATION, "20200101 000000.000");
        params.putSingle(QUERY_BEGIN, BEGIN_DATE);
        params.putSingle(QUERY_END, END_DATE);
        
        QueryParameters queryParams = new QueryParametersImpl();
        queryParams.validate(params);
        
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations(AUTHS));
        
        Query query = new QueryImpl();
        query.initialize(USER, Arrays.asList(USER_DN), null, queryParams, null);
        
        ShardQueryConfiguration config = ShardQueryConfigurationFactory.createShardQueryConfigurationFromConfiguredLogic(logic, query);
        
        logic.getQueryPlanner().setQueryIteratorClass(TestQueryIterator.class);
        
        logic.initialize(config, instance.getConnector("root", PASSWORD), query, auths);
        
        logic.setupQuery(config);
        
        return logic.getTransformIterator(query);
    }
    
    public static class TestFieldIndexFilter extends FieldIndexFilter {
        public static List<Tuple3<String,String,Value>> acceptedFields = new ArrayList<>();
        public static List<Tuple3<String,String,Value>> rejectedFields = new ArrayList<>();
        
        public TestFieldIndexFilter(Map<String,Multimap<String,String>> fieldIndexFilterMapByType, TypeMetadata typeMetadata, JexlArithmetic arithmetic) {
            super(fieldIndexFilterMapByType, typeMetadata, arithmetic);
        }
        
        @Override
        public boolean keep(String ingestType, String fieldName, Value value) {
            boolean result = super.keep(ingestType, fieldName, value);
            if (result) {
                synchronized (acceptedFields) {
                    acceptedFields.add(new Tuple3<>(ingestType, fieldName, value));
                }
            } else {
                synchronized (rejectedFields) {
                    rejectedFields.add(new Tuple3<>(ingestType, fieldName, value));
                }
            }
            return result;
        }
    }
    
    public static class TestIteratorBuildingVisitor extends IteratorBuildingVisitor {
        @Override
        protected FieldIndexFilter createFieldIndexFilter() {
            JexlArithmetic jexlArithmetic = (this.arithmetic instanceof StatefulArithmetic) ? ((StatefulArithmetic) this.arithmetic).clone() : this.arithmetic;
            return new TestFieldIndexFilter(fieldIndexFilterMapByType, typeMetadataWithNonIndexed, jexlArithmetic);
        }
    }
    
    public static class TestQueryIterator extends QueryIterator {
        @Override
        protected IteratorBuildingVisitor createIteratorBuildingVisitor(final Range documentRange, boolean isQueryFullySatisfied, boolean sortedUIDs)
                        throws QuorumPeerConfig.ConfigException, MalformedURLException, InstantiationException, IllegalAccessException {
            return createIteratorBuildingVisitor(TestIteratorBuildingVisitor.class, documentRange, isQueryFullySatisfied, sortedUIDs);
        }
    }
    
    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            
            NormalizedContentInterface fif_nci = new BaseNormalizedContent();
            fif_nci.setFieldName(FIF_ENABLED_FIELD_NAME);
            fif_nci.setEventFieldValue(new String(record.getRawData()));
            fif_nci.setIndexedFieldValue(new String(record.getRawData()));
            eventFields.put(FIF_ENABLED_FIELD_NAME, fif_nci);
            
            NormalizedContentInterface nci = new BaseNormalizedContent();
            nci.setFieldName(FIF_DISABLED_FIELD_NAME);
            nci.setEventFieldValue(new String(record.getRawData()));
            nci.setIndexedFieldValue(new String(record.getRawData()));
            eventFields.put(FIF_DISABLED_FIELD_NAME, nci);
            
            NormalizedFieldAndValue fif_nfv = new NormalizedFieldAndValue(MAPPED_FIELD, Integer.toString(record.getRawData().length));
            eventFields.put(fif_nfv.getEventFieldName(), fif_nfv);
            
            NormalizedFieldAndValue nfv = new NormalizedFieldAndValue(UNMAPPED_FIELD, Integer.toString(record.getRawData().length));
            eventFields.put(nfv.getEventFieldName(), nfv);
            return normalizeMap(eventFields);
        }
    }
}
