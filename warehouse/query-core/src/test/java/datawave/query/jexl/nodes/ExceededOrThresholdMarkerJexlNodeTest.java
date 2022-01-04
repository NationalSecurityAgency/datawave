package datawave.query.jexl.nodes;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.data.type.GeometryType;
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
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownLargeFieldedListsVisitor;
import datawave.query.testframework.MockStatusReporter;
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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.parser.ParseException;
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
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static datawave.webservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.webservice.query.QueryParameters.QUERY_END;
import static datawave.webservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.webservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_PERSISTENCE;
import static datawave.webservice.query.QueryParameters.QUERY_STRING;

@RunWith(Arquillian.class)
public class ExceededOrThresholdMarkerJexlNodeTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private static final int NUM_SHARDS = 1;
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    private static final String GEO_FIELD = "0GEO";
    private static final String GEO_QUERY_FIELD = JexlASTHelper.rebuildIdentifier(GEO_FIELD);
    
    private static final String PASSWORD = "";
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20000101 000001.000";
    
    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";
    
    private static String fstUri;
    
    private static List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
    
    private static final Configuration conf = new Configuration();
    
    private static final String POINT_1 = "POINT (0 0)";
    private static final String POINT_2 = "POINT (0 3)";
    private static final String POINT_3 = "POINT (0 2)";
    private static final String POINT_4 = "POINT (0 1)";
    private static final String POINT_5 = "POINT (1 1)";
    private static final String POINT_6 = "POINT (1 2)";
    private static final String POINT_7 = "POINT (2 2)";
    private static final String POINT_8 = "POINT (2 1)";
    private static final String POINT_9 = "POINT (2 3)";
    private static final String POINT_10 = "POINT (1 3)";
    private static final String POINT_11 = "POINT (2 0)";
    private static final String POINT_12 = "POINT (1 0)";
    private static final String POINT_13 = "POINT (20 20);POINT (20 30)";
    
    private static final String INDEX_1 = "1f0aaaaaaaaaaaaaaa";
    private static final String INDEX_2 = "1f1ffc54fefc54fefc";
    private static final String INDEX_3 = "1f1fffb0ebff104155";
    private static final String INDEX_4 = "1f1fffc410554eb0ff";
    private static final String INDEX_5 = "1f2000228a00228a00";
    private static final String INDEX_6 = "1f2000747900de7300";
    private static final String INDEX_7 = "1f20008a28008a2800";
    private static final String INDEX_8 = "1f2000de7300747900";
    private static final String INDEX_9 = "1f200364bda9c63d03";
    private static final String INDEX_10 = "1f200398c60112ee03";
    private static final String INDEX_11 = "1f35553ac3ffb0ebff";
    private static final String INDEX_12 = "1f35554eb0ffec3aff";
    private static final String INDEX_13_1 = "1f202a02a02a02a02a";
    private static final String INDEX_13_2 = "1f2088888888888888";
    
    // @formatter:off
    private static final String[] wktData = {
            POINT_1,
            POINT_2,
            POINT_3,
            POINT_4,
            POINT_5,
            POINT_6,
            POINT_7,
            POINT_8,
            POINT_9,
            POINT_10,
            POINT_11,
            POINT_12,
            POINT_13};
    // @formatter:on
    
    private int maxOrExpansionThreshold = 1;
    private int maxOrFstThreshold = 1;
    private int maxOrRangeThreshold = 1;
    private int maxOrRangeIvarators = 1;
    private int maxRangesPerRangeIvarator = 1;
    private boolean collapseUids = true;
    
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
    
    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");
        
        fstUri = temporaryFolder.newFolder().toURI().toString();
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString()));
        
        setupConfiguration(conf);
        
        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);
        
        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;
        String beginDate;
        String[] wktData;
        
        beginDate = BEGIN_DATE;
        wktData = ExceededOrThresholdMarkerJexlNodeTest.wktData;
        
        for (int i = 0; i < wktData.length; i++) {
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setDate(formatter.parse(beginDate).getTime());
            record.setRawData((wktData[i]).getBytes("UTF8"));
            record.generateId(null);
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
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD);
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());
        
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
    public void combinedRangesOneIvaratorTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_3 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_5 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_7 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_9 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_11 + "'))";
        // @formatter:on
        
        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 1;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        
        List<String> queryRanges = getQueryRanges(query);
        
        Assert.assertEquals(1, queryRanges.size());
        String id = queryRanges.get(0).substring(queryRanges.get(0).indexOf("id = '") + 6,
                        queryRanges.get(0).indexOf("') && (field = '" + GEO_QUERY_FIELD + "')"));
        Assert.assertEquals(
                        "((_List_ = true) && ((id = '"
                                        + id
                                        + "') && (field = '"
                                        + GEO_QUERY_FIELD
                                        + "') && (params = '{\"ranges\":[[\"[1f0aaaaaaaaaaaaaaa\",\"1f1fffb0ebff104155]\"],[\"[1f2000228a00228a00\",\"1f20008a28008a2800]\"],[\"[1f200364bda9c63d03\",\"1f35553ac3ffb0ebff]\"]]}')))",
                        queryRanges.get(0));
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(10, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void combinedRangesTwoIvaratorsTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_3 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_5 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_7 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_9 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_11 + "'))";
        // @formatter:on
        
        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 100;
        maxOrRangeIvarators = 10;
        maxOrRangeThreshold = 1;
        maxRangesPerRangeIvarator = 2;
        
        List<String> queryRanges = getQueryRanges(query);
        
        Assert.assertEquals(1, queryRanges.size());
        String id = queryRanges.get(0).substring(queryRanges.get(0).indexOf("id = '") + 6,
                        queryRanges.get(0).indexOf("') && (field = '" + GEO_QUERY_FIELD + "')"));
        Assert.assertEquals(
                        "((_Value_ = true) && ((_Bounded_ = true) && ("
                                        + GEO_QUERY_FIELD
                                        + " >= '1f200364bda9c63d03' && "
                                        + GEO_QUERY_FIELD
                                        + " <= '1f35553ac3ffb0ebff'))) || ((_List_ = true) && ((id = '"
                                        + id
                                        + "') && (field = '"
                                        + GEO_QUERY_FIELD
                                        + "') && (params = '{\"ranges\":[[\"[1f0aaaaaaaaaaaaaaa\",\"1f1fffb0ebff104155]\"],[\"[1f2000228a00228a00\",\"1f20008a28008a2800]\"]]}')))",
                        queryRanges.get(0));
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(10, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void combinedRangesWithNegationTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_12 + "')) && " +
                "not(((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_3 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_5 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_7 + "')) || " +
                "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_9 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_11 + "')))";
        // @formatter:on
        
        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 1;
        maxOrRangeIvarators = 10;
        maxRangesPerRangeIvarator = 1;
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(3, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_4, POINT_8, POINT_12));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void valueListTest() throws Exception {
        // @formatter:off
        String query = "(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on
        
        maxOrExpansionThreshold = 1;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(9, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void docSpecificValueListTest() throws Exception {
        // @formatter:off
        String query = "(" + GEO_QUERY_FIELD + " == '" + INDEX_13_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_13_2 + "')";
        // @formatter:on
        
        maxOrExpansionThreshold = 1;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        collapseUids = false;
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(1, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_13.split(";")));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void valueListWithNegationTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_12 + "')) && " +
                "not(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on
        
        maxOrExpansionThreshold = 1;
        maxOrFstThreshold = 100;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(4, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_4, POINT_8, POINT_12));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void fstTest() throws Exception {
        // @formatter:off
        String query = "(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on
        
        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 1;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(9, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_1, POINT_2, POINT_3, POINT_5, POINT_6, POINT_7, POINT_9, POINT_10, POINT_11));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    @Test
    public void fstWithNegationTest() throws Exception {
        // @formatter:off
        String query = "((_Bounded_ = true) && (" + GEO_QUERY_FIELD + " >= '" + INDEX_1 + "' && " + GEO_QUERY_FIELD + " <= '" + INDEX_12 + "')) && " +
                "not(" + GEO_QUERY_FIELD + " == '" + INDEX_1 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_2 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_3 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_5 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_6 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_7 + "' || " +
                "" + GEO_QUERY_FIELD + " == '" + INDEX_9 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_10 + "' || " + GEO_QUERY_FIELD + " == '" + INDEX_11 + "')";
        // @formatter:on
        
        maxOrExpansionThreshold = 100;
        maxOrFstThreshold = 1;
        maxOrRangeThreshold = 100;
        maxOrRangeIvarators = 1;
        maxRangesPerRangeIvarator = 1;
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(4, events.size());
        
        List<String> pointList = new ArrayList<>();
        pointList.addAll(Arrays.asList(POINT_4, POINT_8, POINT_12));
        pointList.addAll(Arrays.asList(POINT_13.split(";")));
        
        for (DefaultEvent event : events) {
            List<String> wkt = new ArrayList<>();
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD))
                    wkt.add(field.getValueString());
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(pointList.removeAll(wkt));
        }
        
        Assert.assertEquals(0, pointList.size());
    }
    
    private List<String> getQueryRanges(String queryString) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic();
        
        Iterator iter = getQueryRangesIterator(queryString, logic);
        List<String> queryData = new ArrayList<>();
        while (iter.hasNext())
            queryData.add((String) iter.next());
        return queryData;
    }
    
    private List<DefaultEvent> getQueryResults(String queryString) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic();
        
        Iterator iter = getResultsIterator(queryString, logic);
        List<DefaultEvent> events = new ArrayList<>();
        while (iter.hasNext())
            events.add((DefaultEvent) iter.next());
        return events;
    }
    
    private Iterator getQueryRangesIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_LOGIC_NAME, "EventQuery");
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "geoQuery");
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
        
        return Iterators.transform(
                        config.getQueries(),
                        queryData -> {
                            try {
                                return JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(config,
                                                JexlASTHelper.parseJexlQuery(queryData.getQuery()), null, null));
                            } catch (ParseException e) {
                                return null;
                            }
                        });
    }
    
    private Iterator getResultsIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_LOGIC_NAME, "EventQuery");
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "geoQuery");
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
    
    private ShardQueryLogic getShardQueryLogic() throws IOException {
        ShardQueryLogic logic = new ShardQueryLogic(this.logic);
        
        // increase the depth threshold
        logic.setMaxDepthThreshold(20);
        
        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setPushdownThreshold(1000000);
        
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());
        
        setupIvarator(logic);
        
        return logic;
    }
    
    private void setupIvarator(ShardQueryLogic logic) throws IOException {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(maxOrExpansionThreshold);
        logic.setMaxOrExpansionFstThreshold(maxOrFstThreshold);
        logic.setMaxOrRangeThreshold(maxOrRangeThreshold);
        logic.setMaxOrRangeIvarators(maxOrRangeIvarators);
        logic.setMaxRangesPerRangeIvarator(maxRangesPerRangeIvarator);
        logic.setIvaratorFstHdfsBaseURIs(fstUri);
        logic.setCollapseUids(collapseUids);
        logic.setIvaratorCacheScanPersistThreshold(1);
        logic.setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);
    }
    
    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            String rawRecord = new String(record.getRawData());
            for (String value : rawRecord.split(";")) {
                NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(GEO_FIELD, value);
                eventFields.put(GEO_FIELD, geo_nci);
            }
            return normalizeMap(eventFields);
        }
    }
}
