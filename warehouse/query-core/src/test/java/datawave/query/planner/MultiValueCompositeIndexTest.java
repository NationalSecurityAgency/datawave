package datawave.query.planner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.data.type.GeometryType;
import datawave.data.type.NumberType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.testframework.MockStatusReporter;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.io.UnsupportedEncodingException;
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

import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_OR_OP;
import static datawave.webservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.webservice.query.QueryParameters.QUERY_END;
import static datawave.webservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.webservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_PERSISTENCE;
import static datawave.webservice.query.QueryParameters.QUERY_STRING;

@RunWith(Arquillian.class)
public class MultiValueCompositeIndexTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private static final int NUM_SHARDS = 241;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    private static final String GEO_FIELD = "GEO";
    private static final String WKT_BYTE_LENGTH_FIELD = "WKT_BYTE_LENGTH";
    
    private static final String PASSWORD = "";
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String COMPOSITE_BEGIN_DATE = "20010101 000000.000";
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20020101 000000.000";
    
    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";
    
    private static final Configuration conf = new Configuration();
    
    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
    
    private static class TestData {
        public TestData(List<String> wktData, List<Integer> numData) {
            this.wktData = wktData;
            this.numData = numData;
        }
        
        public List<String> wktData;
        public List<Integer> numData;
        
        public String toString() {
            return String.join("|", wktData) + "||" + String.join("|", numData.stream().map(x -> x.toString()).collect(Collectors.toList()));
        }
        
        public static TestData fromString(String td) {
            String[] splitData = td.split("\\|\\|");
            return new TestData((splitData.length >= 1) ? Arrays.asList(splitData[0].split("\\|")) : new ArrayList<>(), (splitData.length >= 2) ? Arrays
                            .asList(splitData[1].split("\\|")).stream().map(x -> Integer.parseInt(x)).collect(Collectors.toList()) : new ArrayList<>());
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            
            TestData testData = (TestData) o;
            
            if (!wktData.containsAll(testData.wktData))
                return false;
            if (!testData.wktData.containsAll(wktData))
                return false;
            if (!numData.containsAll(testData.numData))
                return false;
            return testData.numData.containsAll(numData);
        }
        
        @Override
        public int hashCode() {
            int result = wktData.hashCode();
            result = 31 * result + numData.hashCode();
            return result;
        }
    }
    
    private static final List<TestData> testData = new ArrayList<>();
    
    @Inject
    @SpringBean(name = "EventQuery")
    ShardQueryLogic logic;
    
    private static InMemoryInstance instance;
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    public static void createTestData() {
        // Test Data with 2 wkt and 2 numbers
        testData.add(new TestData(Arrays.asList("POLYGON ((-120 20, -100 20, -100 60, -120 60, -120 20))", "POINT (45 -45)"), Arrays.asList(55, 15)));
        
        // Test Data with 2 wkt and 1 number
        testData.add(new TestData(Arrays.asList("POLYGON ((-110 -15, -105 -15, -105 -10, -110 -10, -110 -15))", "POINT (45 45)"), Collections.singletonList(60)));
        
        // Test Data with 1 wkt and 2 numbers
        testData.add(new TestData(Collections.singletonList("POINT (0 0)"), Arrays.asList(11, 22)));
        
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");
        
        createTestData();
        
        setupConfiguration(conf);
        
        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);
        
        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;
        
        for (int i = 0; i < testData.size(); i++) {
            TestData entry = testData.get(i);
            
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setDate(formatter.parse(COMPOSITE_BEGIN_DATE).getTime());
            record.setRawData(entry.toString().getBytes("UTF8"));
            record.generateId(null);
            record.setVisibility(new ColumnVisibility(AUTHS));
            
            final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);
            
            Multimap<String,NormalizedContentInterface> compositeFields = ingestHelper.getCompositeFields(fields);
            for (String fieldName : compositeFields.keySet()) {
                // if this is an overloaded event field, we are replacing the existing data
                if (ingestHelper.isOverloadedCompositeField(fieldName))
                    fields.removeAll(fieldName);
                fields.putAll(fieldName, compositeFields.get(fieldName));
            }
            
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
        
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString()));
    }
    
    public static void setupConfiguration(Configuration conf) {
        String compositeFieldName = GEO_FIELD;
        conf.set(DATA_TYPE_NAME + "." + compositeFieldName + BaseIngestHelper.COMPOSITE_FIELD_MAP,
                        String.join(",", new String[] {GEO_FIELD, WKT_BYTE_LENGTH_FIELD}));
        
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD + ((!compositeFieldName.equals(GEO_FIELD)) ? "," + compositeFieldName : ""));
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + WKT_BYTE_LENGTH_FIELD + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        
        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        conf.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, INGEST_HELPER_CLASS);
        
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
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_ENABLED, "false");
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_CONFIG, "");
        conf.set("partitioner.category.shardedTables", BalancedShardPartitioner.class.getName());
        conf.set("partitioner.category.member." + TableName.SHARD, "shardedTables");
    }
    
    private static void writeKeyValues(Connector connector, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = connector.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName)) {
                tops.create(tableName);
            }
            
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
    public void compositeWithoutIvaratorTest() throws Exception {
        // @formatter:off
        String query = "(((_Bounded_ = true) && (" + GEO_FIELD + " >= '0311'" + JEXL_AND_OP + GEO_FIELD + " <= '0312'))" + JEXL_AND_OP +
                WKT_BYTE_LENGTH_FIELD + " == 15)" + JEXL_OR_OP +
                "(" + GEO_FIELD + " == '1f20aaaaaaaaaaaaaa'" + JEXL_AND_OP +
                "((_Bounded_ = true) && (" + WKT_BYTE_LENGTH_FIELD + " >= 59" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " <= 61)))" + JEXL_OR_OP +
                "(" + GEO_FIELD + " == '1f0aaaaaaaaaaaaaaa'" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " >= 22)";
        // @formatter:on
        
        List<QueryData> queries = getQueryRanges(query, false);
        Assert.assertEquals(3, queries.size());
        
        List<DefaultEvent> events = getQueryResults(query, false);
        Assert.assertEquals(3, events.size());
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            List<Integer> wktByteLength = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
                else if (field.getName().equals(WKT_BYTE_LENGTH_FIELD))
                    wktByteLength.add(Integer.parseInt(field.getValueString()));
            }
            
            TestData result = new TestData(wkt, wktByteLength);
            Assert.assertTrue(testData.contains(result));
        }
        
        Assert.assertEquals(3, events.size());
    }
    
    @Test
    public void compositeWithIvaratorTest() throws Exception {
        // @formatter:off
        String query = "(((_Bounded_ = true) && (" + GEO_FIELD + " >= '0311'" + JEXL_AND_OP + GEO_FIELD + " <= '0312'))" + JEXL_AND_OP +
                       WKT_BYTE_LENGTH_FIELD + " == 15)" + JEXL_OR_OP +
                       "(" + GEO_FIELD + " == '1f20aaaaaaaaaaaaaa'" + JEXL_AND_OP +
                       "((_Bounded_ = true) && (" + WKT_BYTE_LENGTH_FIELD + " >= 59" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " <= 61)))" + JEXL_OR_OP +
                       "(" + GEO_FIELD + " == '1f0aaaaaaaaaaaaaaa'" + JEXL_AND_OP + WKT_BYTE_LENGTH_FIELD + " >= 22)";
        // @formatter:on
        
        List<QueryData> queries = getQueryRanges(query, true);
        Assert.assertEquals(732, queries.size());
        
        List<DefaultEvent> events = getQueryResults(query, true);
        Assert.assertEquals(3, events.size());
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            List<Integer> wktByteLength = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
                else if (field.getName().equals(WKT_BYTE_LENGTH_FIELD))
                    wktByteLength.add(Integer.parseInt(field.getValueString()));
            }
            
            TestData result = new TestData(wkt, wktByteLength);
            Assert.assertTrue(testData.contains(result));
        }
        
        Assert.assertEquals(3, events.size());
    }
    
    private List<QueryData> getQueryRanges(String queryString, boolean useIvarator) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic(useIvarator);
        
        Iterator iter = getQueryRangesIterator(queryString, logic);
        List<QueryData> queryData = new ArrayList<>();
        while (iter.hasNext())
            queryData.add((QueryData) iter.next());
        return queryData;
    }
    
    private List<DefaultEvent> getQueryResults(String queryString, boolean useIvarator) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic(useIvarator);
        
        Iterator iter = getResultsIterator(queryString, logic);
        List<DefaultEvent> events = new ArrayList<>();
        while (iter.hasNext())
            events.add((DefaultEvent) iter.next());
        return events;
    }
    
    private Iterator getQueryRangesIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "geoQuery");
        params.putSingle(QUERY_LOGIC_NAME, "EventQueryLogic");
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
        
        ShardQueryConfiguration config = ShardQueryConfiguration.create(logic, query);
        
        logic.initialize(config, instance.getConnector("root", PASSWORD), query, auths);
        
        logic.setupQuery(config);
        
        return config.getQueries();
    }
    
    private Iterator getResultsIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "geoQuery");
        params.putSingle(QUERY_LOGIC_NAME, "EventQueryLogic");
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
    
    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            
            try {
                TestData entry = TestData.fromString(new String(record.getRawData(), "UTF8"));
                
                for (int i = 0; i < entry.wktData.size(); i++) {
                    NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(GEO_FIELD, entry.wktData.get(i), Integer.toString(i), null);
                    eventFields.put(GEO_FIELD, geo_nci);
                }
                
                for (int i = 0; i < entry.numData.size(); i++) {
                    NormalizedContentInterface wktByteLength_nci = new NormalizedFieldAndValue(WKT_BYTE_LENGTH_FIELD, Integer.toString(entry.numData.get(i)),
                                    Integer.toString(i), null);
                    eventFields.put(WKT_BYTE_LENGTH_FIELD, wktByteLength_nci);
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            
            return normalizeMap(eventFields);
        }
    }
}
