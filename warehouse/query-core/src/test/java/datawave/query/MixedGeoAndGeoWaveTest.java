package datawave.query;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.data.type.GeoType;
import datawave.data.type.PointType;
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
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.testframework.MockStatusReporter;
import datawave.query.model.QueryModel;
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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.util.GeometricShapeFactory;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
public class MixedGeoAndGeoWaveTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private static final int NUM_CIRCLE_POINTS = 60;
    private static final int NUM_SHARDS = 100;
    private static final String DATA_TYPE_NAME = "MixedGeo";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    private static final String GEO_FIELD = "GEO";
    private static final String POINT_FIELD = "POINT";
    private static final String POLY_POINT_FIELD = "POLY_POINT";
    
    private static final String PASSWORD = "";
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String MID_DATE = "20010101 000000.000";
    private static final String END_DATE = "20020101 000000.000";
    
    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";
    
    private static final Configuration conf = new Configuration();
    
    private static final String GEO_1 = "0_0";
    private static final String GEO_2 = "3_0";
    private static final String GEO_3 = "2_0";
    private static final String GEO_4 = "1_0";
    private static final String GEO_5 = "1_1";
    private static final String GEO_6 = "2_1";
    
    private static final String POINT_1 = "POINT (2 2)";
    private static final String POINT_2 = "POINT (2 1)";
    private static final String POINT_3 = "POINT (2 3)";
    private static final String POINT_4 = "POINT (1 3)";
    private static final String POINT_5 = "POINT (2 0)";
    private static final String POINT_6 = "POINT (1 0)";
    
    private static final String POLY_1 = "POLYGON((-4 -4, 0 -4, 0 0, -4 0, -4 -4))";
    private static final String POLY_2 = "POLYGON((0 -4, 4 -4, 4 0, -4 0, 0 -4))";
    private static final String POLY_3 = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";
    private static final String POLY_4 = "POLYGON((-4 0, 0 0, 0 4, -4 4, -4 0))";
    private static final String POLY_5 = "POLYGON((-2 -2, 2 -2, 2 2, -2 2, -2 -2))";
    
    // @formatter:off
    private static final String[] geoData = {
            GEO_1,
            GEO_2,
            GEO_3,
            GEO_4,
            GEO_5,
            GEO_6};
    // @formatter:on
    
    // @formatter:off
    private static final String[] pointData = {
            POINT_1,
            POINT_2,
            POINT_3,
            POINT_4,
            POINT_5,
            POINT_6};
    // @formatter:on
    
    // @formatter:off
    private static final String[] polyData = {
            POLY_1,
            POLY_2,
            POLY_3,
            POLY_4,
            POLY_5};
    // @formatter:on
    
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
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");
        
        instance = new InMemoryInstance();
        
        int recNum = 1;
        
        setupConfiguration(conf);
        recNum = ingestData(conf, GEO_FIELD, geoData, recNum, BEGIN_DATE);
        recNum = ingestData(conf, POINT_FIELD, pointData, recNum, MID_DATE);
        ingestData(conf, POLY_POINT_FIELD, polyData, recNum, MID_DATE);
        
        ivaratorCacheDirConfigs = Collections.singletonList(new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString()));
    }
    
    public static int ingestData(Configuration conf, String fieldName, String[] data, int startRecNum, String ingestDate) throws Exception {
        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);
        
        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = startRecNum;
        
        for (int i = 0; i < data.length; i++) {
            record.clear();
            record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
            record.setRawFileName("geodata_" + recNum + ".dat");
            record.setRawRecordNumber(recNum++);
            record.setDate(formatter.parse(ingestDate).getTime());
            record.setRawData((fieldName + data[i]).getBytes("UTF8"));
            record.generateId(null);
            record.setVisibility(new ColumnVisibility(AUTHS));
            
            final Multimap<String,NormalizedContentInterface> fields = LinkedListMultimap.create();
            for (Map.Entry<String,NormalizedContentInterface> entry : ingestHelper.getEventFields(record).entries())
                if (entry.getValue().getError() == null)
                    fields.put(entry.getKey(), entry.getValue());
            
            Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());
            
            keyValues.putAll(kvPairs);
            
            dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
        }
        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());
        
        // write these values to their respective tables
        Connector connector = instance.getConnector("root", PASSWORD);
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));
        
        writeKeyValues(connector, keyValues);
        
        return recNum;
    }
    
    public static void setupConfiguration(Configuration conf) {
        conf.clear();
        
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD + "," + POINT_FIELD + "," + POLY_POINT_FIELD);
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeoType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + POINT_FIELD + BaseIngestHelper.FIELD_TYPE, PointType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + POLY_POINT_FIELD + BaseIngestHelper.FIELD_TYPE, PointType.class.getName());
        
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
    public void withinSmallBoundingBoxTest() throws Exception {
        String query = "geo:within_bounding_box(" + GEO_FIELD + ", '2_0.5', '10_1.5')";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(2, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(GEO_6, POINT_4));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void intersectsSmallBoundingBoxTest() throws Exception {
        String query = "geowave:intersects(" + GEO_FIELD + ", 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))')";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(2, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(GEO_6, POINT_4));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinSmallBoundingBoxEvaluationOnlyTest() throws Exception {
        String query = "geo:within_bounding_box(" + GEO_FIELD + ", '2_0.5', '10_1.5') && ((_Eval_ = true) && geo:within_bounding_box(" + GEO_FIELD
                        + ", '2_0.5', '10_1.5'))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(2, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(GEO_6, POINT_4));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void intersectsSmallBoundingBoxEvaluationOnlyTest() throws Exception {
        String query = "geowave:intersects(" + GEO_FIELD
                        + ", 'POLYGON((0.5 2, 0.5 10, 1.5 10, 1.5 2, 0.5 2))') && ((ASTEvaluationOnly = true) && geowave:intersects(" + GEO_FIELD
                        + ", 'POLYGON((0.5 2, 0.5 10, 1.5 10, 1.5 2, 0.5 2))'))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(2, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(GEO_6, POINT_4));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinLargeBoundingBoxTest() throws Exception {
        String query = "geo:within_bounding_box(" + GEO_FIELD + ", '-90_-180', '90_180')";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void intersectsLargeBoundingBoxTest() throws Exception {
        String query = "geowave:intersects(" + GEO_FIELD + ", 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinLargeBoundingBoxEvaluationOnlyTest() throws Exception {
        String query = "geo:within_bounding_box(" + GEO_FIELD + ", '-90_-180', '90_180') && ((_Eval_ = true) && geo:within_bounding_box(" + GEO_FIELD
                        + ", '-90_-180', '90_180'))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void intersectsLargeBoundingBoxEvaluationOnlyTest() throws Exception {
        String query = "geowave:intersects(" + GEO_FIELD
                        + ", 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))') && ((ASTEvaluationOnly = true) && geowave:intersects(" + GEO_FIELD
                        + ", 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))'))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinLargeCircleTest() throws Exception {
        String query = "geo:within_circle(" + GEO_FIELD + ", '0_0', 90)";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void intersectsLargeCircleTest() throws Exception {
        String query = "geowave:intersects(" + GEO_FIELD + ", '" + createCircle(0, 0, 90).toText() + "')";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinLargeCircleEvaluationOnlyTest() throws Exception {
        String query = "geo:within_circle(" + GEO_FIELD + ", '0_0', 90) && ((_Eval_ = true) && geo:within_circle(" + GEO_FIELD + ", '0_0', 90))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void intersectsLargeCircleEvaluationOnlyTest() throws Exception {
        String query = "geowave:intersects(" + GEO_FIELD + ", '" + createCircle(0, 0, 90).toText() + "') && ((ASTEvaluationOnly = true) && geowave:intersects("
                        + GEO_FIELD + ", '" + createCircle(0, 0, 90).toText() + "'))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(12, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(geoData));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinLargeBoundingBoxAcrossAntimeridianTest() throws Exception {
        String query = "geo:within_bounding_box(" + GEO_FIELD + ", '-90_0.01', '90_-0.01')";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(8, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(GEO_5, GEO_6));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    @Test
    public void withinLargeBoundingBoxAcrossAntimeridianEvaluationOnlyTest() throws Exception {
        String query = "geo:within_bounding_box(" + GEO_FIELD + ", '-90_0.01', '90_-0.01') && ((_Eval_ = true) && geo:within_bounding_box(" + GEO_FIELD
                        + ", '-90_0.01', '90_-0.01'))";
        
        List<DefaultEvent> events = getQueryResults(query);
        Assert.assertEquals(8, events.size());
        
        List<String> geoList = new ArrayList<>();
        geoList.addAll(Arrays.asList(pointData));
        geoList.addAll(Arrays.asList(GEO_5, GEO_6));
        
        for (DefaultEvent event : events) {
            String geo = null;
            
            for (DefaultField field : event.getFields()) {
                if (field.getName().equals(GEO_FIELD) || field.getName().equals(POINT_FIELD))
                    geo = field.getValueString();
            }
            
            // ensure that this is one of the ingested events
            Assert.assertTrue(geoList.remove(geo));
        }
        
        Assert.assertEquals(0, geoList.size());
    }
    
    // Note: Trying to ingest non-point WKT as PointType will not work. PointType can only be used for POINT wkt
    @Test(expected = InvalidQueryException.class)
    public void polyPointTest() throws Exception {
        String query = "geo:within_bounding_box(" + POLY_POINT_FIELD + ", '-1_-1', '1_1')";
        getQueryResults(query);
    }
    
    private Polygon createCircle(double lon, double lat, double radius) {
        GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
        shapeFactory.setNumPoints(NUM_CIRCLE_POINTS);
        shapeFactory.setCentre(new Coordinate(lon, lat));
        shapeFactory.setSize(radius * 2);
        return shapeFactory.createCircle();
    }
    
    private List<DefaultEvent> getQueryResults(String queryString) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic();
        
        Iterator iter = getResultsIterator(queryString, logic);
        List<DefaultEvent> events = new ArrayList<>();
        while (iter.hasNext())
            events.add((DefaultEvent) iter.next());
        return events;
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
        
        QueryModel queryModel = new QueryModel();
        queryModel.addTermToModel(GEO_FIELD, GEO_FIELD);
        queryModel.addTermToModel(GEO_FIELD, POINT_FIELD);
        config.setQueryModel(queryModel);
        
        logic.initialize(config, instance.getConnector("root", PASSWORD), query, auths);
        
        logic.setupQuery(config);
        
        return logic.getTransformIterator(query);
    }
    
    private ShardQueryLogic getShardQueryLogic() {
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
    
    private void setupIvarator(ShardQueryLogic logic) {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setIvaratorCacheScanPersistThreshold(1);
        logic.setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);
    }
    
    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            String rawData = new String(record.getRawData());
            String prefix = null;
            if (rawData.startsWith(GEO_FIELD)) {
                prefix = GEO_FIELD;
            } else if (rawData.startsWith(POINT_FIELD)) {
                prefix = POINT_FIELD;
            } else if (rawData.startsWith(POLY_POINT_FIELD)) {
                prefix = POLY_POINT_FIELD;
            }
            NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(prefix, rawData.substring(prefix.length()));
            eventFields.put(prefix, geo_nci);
            return normalizeMap(eventFields);
        }
    }
}
