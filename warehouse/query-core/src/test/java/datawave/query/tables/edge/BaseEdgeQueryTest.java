package datawave.query.tables.edge;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import datawave.data.normalizer.Normalizer;
import datawave.query.MockAccumuloRecordWriter;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.BaseQueryLogic;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * A base test class to encapsulate everything needed to run query tests against an edge query logic.
 */
public abstract class BaseEdgeQueryTest {
    
    protected static final boolean protobufEdgeFormat = true;
    
    public static final String EDGE_TABLE_NAME = "edge";
    public static final String MODEL_TABLE_NAME = "DatawaveMetadata";
    
    protected static SimpleDateFormat simpleFormat;
    
    protected static Connector connector;
    protected static datawave.query.MockAccumuloRecordWriter recordWriter;
    protected Set<Authorizations> auths = Collections.singleton(new Authorizations("A", "B", "C", "D"));
    protected Set<Authorizations> limitedAuths = Collections.singleton(new Authorizations("A", "B"));
    
    protected static final String UNEXPECTED_NUM_RECORDS = "Did not receive the expected number of records.";
    protected static final String UNEXPECTED_RECORD = "Found an unexpected record.";
    
    private String serializeAuths(Set<Authorizations> sentAuths) {
        Assert.assertEquals(1, sentAuths.size());
        return sentAuths.iterator().next().serialize();
    }
    
    public QueryImpl configQuery(String query, Set<Authorizations> auths) throws ParseException {
        Date startDate = simpleFormat.parse("20140701");
        Date endDate = simpleFormat.parse("20150730");
        Map<String,String> extraParams = new HashMap<>();
        QueryImpl q = new QueryImpl();
        q.setBeginDate(startDate);
        q.setEndDate(endDate);
        q.setQuery(query);
        q.setParameters(extraParams);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(serializeAuths(auths));
        q.addParameter("model.name", "something");
        return q;
    }
    
    private static TestEdge createEdge(String source, String sink, String date, String edgeType, String sourceReleation, String sinkrelation,
                    String sourceSource, String sinkSource, String visibility, long timestamp) {
        
        return TestEdge.createEdge(Normalizer.LC_NO_DIACRITICS_NORMALIZER.normalize(source), Normalizer.LC_NO_DIACRITICS_NORMALIZER.normalize(sink), date,
                        edgeType, sourceReleation, sinkrelation, sourceSource, sinkSource, visibility, timestamp);
    }
    
    public static TestEdge createEdge(String source, String dateStr, String statsType, String dataType, String toRel, String toSource, String attr2,
                    String visibility, long timestamp) {
        
        return TestEdge.createEdge(Normalizer.LC_NO_DIACRITICS_NORMALIZER.normalize(source), dateStr, statsType, dataType, toRel, toSource, visibility,
                        timestamp);
    }
    
    public static List<TestEdge> createEdges(String yyyyMMdd) {
        Date date = new Date();
        long timestamp = date.getTime();
        ArrayList<TestEdge> retVal = new ArrayList<>();
        retVal.add(createEdge("SUN", "MERCURY", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("EARTH", "MOON", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("CERES", "JUPITER", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("ASTEROID_BELT", "CERES", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("ASTEROID_BELT", "JUPITER", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("ASTEROID_BELT", "MARS", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("MARS", "CERES", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("HAUMEA", "NAMAKA", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("PLUTO", "CHARON", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "NEW_HORIZONS", "NEW_HORIZONS", "C", timestamp));
        retVal.add(createEdge("ERIS", "DYSNOMIA", yyyyMMdd, "AdjacentCelestialBodies", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        
        retVal.add(createEdge("MERCURY", "VENUS", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("VENUS", "EARTH", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("EARTH", "MARS", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("MARS", "JUPITER", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("JUPITER", "SATURN", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("SATURN", "NEPTUNE", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("NEPTUNE", "PLUTO", yyyyMMdd, "AdjacentPlanets", "FROM", "TO", "NEW_HORIZONS", "NEW_HORIZONS", "C", timestamp));
        
        retVal.add(createEdge("PLUTO", "NEPTUNE", yyyyMMdd, "AdjacentDwarfPlanets", "FROM", "TO", "NEW_HORIZONS", "NEW_HORIZONS", "C", timestamp));
        retVal.add(createEdge("CERES", "MARS", yyyyMMdd, "AdjacentDwarfPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("CERES", "JUPITER", yyyyMMdd, "AdjacentDwarfPlanets", "FROM", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        
        // Stats edges
        retVal.add(createEdge("SUN", yyyyMMdd, "ACTIVITY", "Stars", "TO", "COSMOS_DATA", "COSMOS_DATA", "A", timestamp));
        retVal.add(createEdge("MERCURY", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("VENUS", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("EARTH", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("MARS", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("JUPITER", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("SATURN", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("URANUS", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("NEPTUNE", yyyyMMdd, "ACTIVITY", "Planets", "TO", "COSMOS_DATA", "COSMOS_DATA", "B", timestamp));
        retVal.add(createEdge("PLUTO", yyyyMMdd, "ACTIVITY", "DwarfPlanets", "TO", "NEW_HORIZONS", "NEW_HORIZONS", "D", timestamp));
        retVal.add(createEdge("CERES", yyyyMMdd, "ACTIVITY", "DwarfPlanets", "TO", "COSMOS_DATA", "COSMOS_DATA", "D", timestamp));
        retVal.add(createEdge("ERIS", yyyyMMdd, "ACTIVITY", "DwarfPlanets", "TO", "COSMOS_DATA", "COSMOS_DATA", "D", timestamp));
        
        return retVal;
    }
    
    public void compareResults(BaseQueryLogic<Map.Entry<Key,Value>> logic, List<String> expected) {
        int recordsFound = 0;
        List<Key> foundKeys = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : logic) {
            foundKeys.add(entry.getKey());
            Key k = entry.getKey();
            System.out.println("key = " + k.toStringNoTime());
            Assert.assertTrue(UNEXPECTED_RECORD + " : " + k.toStringNoTime(), expected.contains(k.toStringNoTime()));
            recordsFound++;
        }
        
        Assert.assertEquals(UNEXPECTED_NUM_RECORDS, expected.size(), recordsFound);
    }
    
    public static void addEdges() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        TaskAttemptID id = new TaskAttemptID();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, id);
        
        Text edgeTableName = new Text(EDGE_TABLE_NAME);
        List<TestEdge> edges = new ArrayList<>();
        edges.addAll(createEdges("20150713"));
        edges.addAll(createEdges("20170713"));
        edges.addAll(createEdges("20190713"));
        for (TestEdge edge : edges) {
            List<Mutation> mutants = edge.getMutations(protobufEdgeFormat);
            for (Mutation mut : mutants) {
                recordWriter.write(edgeTableName, mut);
            }
        }
        recordWriter.close(context);
    }
    
    @BeforeClass
    public static void setUp() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
        
        // Need to set SDFs after setting the timezone to GMT, else dates will be EST and converted to GMT
        simpleFormat = new SimpleDateFormat("yyyyMMdd");
        
        InMemoryInstance i = new InMemoryInstance(BaseEdgeQueryTest.class.toString());
        connector = i.getConnector("root", new PasswordToken(""));
        
        // Create the CB tables
        connector.tableOperations().create(EDGE_TABLE_NAME);
        connector.tableOperations().create(MODEL_TABLE_NAME);
        
        // Create the map of batchwriters to cb tables
        recordWriter = new MockAccumuloRecordWriter();
        BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
        recordWriter.addWriter(new Text(EDGE_TABLE_NAME), connector.createBatchWriter(EDGE_TABLE_NAME, bwCfg));
        
        addEdges();
    }
}
