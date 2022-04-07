package datawave.query.cardinality;

import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.AbstractQueryLogicTransformer;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestCardinalityWithQuery {
    
    static InMemoryInstance instance;
    private static Connector connector;
    private static ShardQueryLogic logic;
    static BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
    
    private static long timestamp;
    static String METADATA_TABLE_NAME = "metadata";
    static String SHARD_TABLE_NAME = "shard";
    static String SHARD_INDEX_TABLE_NAME = "shardIndex";
    
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    
    private DatawavePrincipal datawavePrincipal;
    private Authorizations auths = new Authorizations("STUFF,THINGS");
    
    private static final Value NULL_VALUE = new Value(new byte[0]);
    
    private Path temporaryFolder;
    
    @BeforeClass
    public static void setUp() throws Exception {
        instance = new InMemoryInstance();
        connector = instance.getConnector("root", new PasswordToken(""));
        
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = format.parse("20190101");
        timestamp = date.getTime();
        
        connector.tableOperations().create(SHARD_TABLE_NAME);
        connector.tableOperations().create(SHARD_INDEX_TABLE_NAME);
        connector.tableOperations().create(METADATA_TABLE_NAME);
        
    }
    
    @Before
    public void setup() throws Exception {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        temporaryFolder = tempDir.newFolder().toPath();
        
        logic = new ShardQueryLogic();
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        
        QueryTestTableHelper.configureLogicToScanTables(logic);
        loadData();
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
        datawavePrincipal = new DatawavePrincipal(Collections.singleton(user));
    }
    
    @After
    public void cleanUp() {
        temporaryFolder.toFile().deleteOnExit();
    }
    
    private void loadData() throws Exception {
        String datatype = "mytype";
        Text colf = new Text(datatype + "\u0000mystuff");
        ColumnVisibility vis = new ColumnVisibility("");
        
        // metadata
        BatchWriter bw = connector.createBatchWriter(METADATA_TABLE_NAME, bwConfig);
        
        Mutation m = new Mutation("ID");
        m.put(new Text("e"), new Text(datatype), timestamp, NULL_VALUE);
        m.put(new Text("i"), new Text(datatype), timestamp, NULL_VALUE);
        m.put(new Text("t"), new Text(datatype + "\0datawave.data.type.NoOpType"), timestamp, NULL_VALUE);
        bw.addMutation(m);
        
        m = new Mutation("DATE");
        m.put(new Text("e"), new Text(datatype), timestamp, NULL_VALUE);
        bw.addMutation(m);
        
        m = new Mutation("FIELD");
        m.put(new Text("e"), new Text(datatype), timestamp, NULL_VALUE);
        bw.addMutation(m);
        bw.close();
        
        // shard
        bw = connector.createBatchWriter(SHARD_TABLE_NAME, bwConfig);
        
        m = new Mutation("20190101_0");
        m.put(colf, new Text("ID\u0000id-001"), vis, timestamp, NULL_VALUE);
        m.put(colf, new Text("DATE\u000020190102"), vis, timestamp, NULL_VALUE);
        m.put(colf, new Text("FIELD\u0000value"), vis, timestamp, NULL_VALUE);
        
        m.put(new Text("fi\u0000ID"), new Text("id-001\u0000" + colf.toString()), vis, timestamp, NULL_VALUE);
        m.put(new Text("fi\u0000FIELD"), new Text("value\u0000" + colf.toString()), vis, timestamp, NULL_VALUE);
        
        bw.addMutation(m);
        bw.close();
        
        // shardIndex
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addUID("mystuff");
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        Uid.List list = builder.build();
        
        bw = connector.createBatchWriter(SHARD_INDEX_TABLE_NAME, bwConfig);
        m = new Mutation("id-001");
        m.put(new Text("ID"), new Text("20190101_0\u0000mytype"), vis, timestamp, new Value(list.toByteArray()));
        bw.addMutation(m);
        bw.close();
        
    }
    
    @Test
    public void runQuery() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        QueryImpl q = new QueryImpl();
        q.setBeginDate(format.parse("20180101"));
        q.setEndDate(format.parse("20200101"));
        q.setQuery("ID == 'id-001'");
        q.setQueryAuthorizations(auths.toString());
        q.setPagesize(Integer.MAX_VALUE);
        q.setId(UUID.randomUUID());
        
        /*
         * The reduced response parameter causes components of the attribute metadata, including timestamp, not to be written or read when serializing and
         * deserializing
         */
        q.addParameter("reduced.response", "true");
        q.setQueryLogicName("EventQuery");
        
        RunningQuery query = new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, logic, q, "", datawavePrincipal,
                        new QueryMetricFactoryImpl());
        TransformIterator<?,?> it = query.getTransformIterator();
        AbstractQueryLogicTransformer<?,?> et = (DocumentTransformer) it.getTransformer();
        
        CardinalityConfiguration cconf = new CardinalityConfiguration();
        Set<String> cardFields = new HashSet<String>();
        cardFields.add("ID");
        cconf.setCardinalityFields(cardFields);
        
        Map<String,String> cmap = new HashMap<String,String>();
        cmap.put("ID", "ID");
        cconf.setCardinalityFieldReverseMapping(cmap);
        
        String path = temporaryFolder.toAbsolutePath().toString();
        
        cconf.setOutputFileDirectory(path);
        cconf.setCardinalityUidField("ID");
        cconf.setFlushThreshold(1);
        
        ((DocumentTransformer) it.getTransformer()).setCardinalityConfiguration(cconf);
        EventQueryResponseBase response = (EventQueryResponseBase) et.createResponse(query.next());
        
        // Wait for cardinality info to be written to the file
        Thread.currentThread().sleep(1000);
        
        Assert.assertTrue(response.getReturnedEvents().intValue() > 0);
        
        boolean createdDocFile = false;
        boolean readDocFile = false;
        File folder = new File(path);
        for (File f : folder.listFiles()) {
            if (f.getName().contains("document")) {
                createdDocFile = true;
                CardinalityRecord rcc = CardinalityRecord.readFromDisk(f);
                if (null != rcc) {
                    for (DateFieldValueCardinalityRecord dr : rcc.getCardinalityMap().values()) {
                        Assert.assertEquals("20190101", dr.getEventDate());
                        readDocFile = true;
                    }
                }
            }
        }
        Assert.assertTrue(createdDocFile);
        Assert.assertTrue(readDocFile);
        
    }
    
}
