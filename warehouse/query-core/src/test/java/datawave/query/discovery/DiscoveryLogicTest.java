package datawave.query.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.TableName;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DiscoveryLogicTest {
    private static Logger log = Logger.getLogger(DiscoveryLogicTest.class);
    
    protected static Set<Pair<String,String>> terms;
    protected static Set<Pair<String,String>> terms2;
    protected static Value blank;
    
    protected static Set<Authorizations> auths = Collections.singleton(new Authorizations("FOO", "BAR"));
    protected static String queryAuths = "FOO,BAR";
    protected Connector connector = null;
    protected MockAccumuloRecordWriter recordWriter;
    protected DiscoveryLogic logic;
    protected SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    
    @BeforeClass
    public static void setUp() {
        blank = new Value(new byte[0]);
        terms = Sets.newHashSet(Pair.with("firetruck", "vehicle"), Pair.with("ruddy duck", "bird"), Pair.with("ruddy duck", "unidentified flying object"),
                        Pair.with("motorcycle", "vehicle"), Pair.with("motorboat", "vehicle"), Pair.with("strike", "actionable offense"),
                        Pair.with("car", "vehicle"), Pair.with("trophy", "prize"), Pair.with("police officer", "otherperson"),
                        Pair.with("skydiver", "occupation"), Pair.with("bbc", "network"), Pair.with("onyx", "pokemon"), Pair.with("onyx", "rock"),
                        Pair.with("onyx", "rooster"), Pair.with("rooster", "cockadoodledoo"));
        
        terms2 = Sets.newHashSet(Pair.with("skydiver", "job"), Pair.with("skydiver", "job"), Pair.with("skydiver", "job"), Pair.with("skydiver", "job"),
                        Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"),
                        Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"),
                        Pair.with("skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"),
                        Pair.with("xxx.skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"),
                        Pair.with("yyy.skydiver", "occupation"), Pair.with("yyy.skydiver", "occupation"), Pair.with("yyy.skydiver", "occupation"),
                        Pair.with("zskydiver", "occupation"));
        System.setProperty(MetadataHelperFactory.ALL_AUTHS_PROPERTY, queryAuths);
    }
    
    @Before
    public void setup() throws Throwable {
        QueryTestTableHelper testTableHelper = new QueryTestTableHelper(DiscoveryLogicTest.class.getCanonicalName(), log);
        recordWriter = new MockAccumuloRecordWriter();
        testTableHelper.configureTables(recordWriter);
        connector = testTableHelper.connector;
        
        for (Pair p : terms) {
            insertIndex(p);
        }
        
        insertForwardModel("animal", "rooster");
        insertForwardModel("animal", "bird");
        insertReverseModel("occupation", "job");
        
        logic = new DiscoveryLogic();
        logic.setIndexTableName(TableName.SHARD_INDEX);
        logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
        logic.setModelTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setModelName("DATAWAVE");
        logic.setFullTableScanEnabled(false);
        logic.setMaxResults(-1);
        logic.setMaxWork(-1);
        logic.setAllowLeadingWildcard(true);
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
    }
    
    protected Uid.List makeUidList(int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(count);
        return builder.build();
    }
    
    protected void insertIndex(Pair<String,String> valueField) throws Throwable {
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        ColumnVisibility viz = new ColumnVisibility("FOO");
        
        List<Date> dates = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            dates.add(dateFormatter.parse("2013010" + i));
        }
        
        try (BatchWriter writer = connector.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, config)) {
            Mutation m = new Mutation(valueField.getValue1().toUpperCase());
            m.put("t", "datatype\u0000" + LcNoDiacriticsType.class.getName(), viz, blank);
            m.put("i", "datatype", viz, blank);
            m.put("ri", "datatype", viz, blank);
            writer.addMutation(m);
        }
        
        try (BatchWriter writer = connector.createBatchWriter(TableName.SHARD_INDEX, config)) {
            Mutation m = new Mutation(valueField.getValue0().toLowerCase());
            int numShards = 10;
            for (int i = 0; i < numShards; i++) {
                for (Date date : dates) {
                    String shard = dateFormatter.format(date);
                    m.put(valueField.getValue1().toUpperCase(), shard + "_" + i + "\u0000datatype", viz, date.getTime(), new Value(makeUidList(24)
                                    .toByteArray()));
                }
            }
            writer.addMutation(m);
        }
        
        try (BatchWriter writer = connector.createBatchWriter(TableName.SHARD_RINDEX, config)) {
            Mutation m = new Mutation(new StringBuilder().append(valueField.getValue0().toLowerCase()).reverse().toString());
            int numShards = 10;
            for (int i = 0; i < numShards; i++) {
                for (Date date : dates) {
                    String shard = dateFormatter.format(date);
                    m.put(valueField.getValue1().toUpperCase(), shard + "_" + i + "\u0000datatype", viz, date.getTime(), new Value(makeUidList(24)
                                    .toByteArray()));
                }
            }
            writer.addMutation(m);
        }
    }
    
    protected void insertForwardModel(String from, String to) throws Throwable {
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        ColumnVisibility viz = new ColumnVisibility("FOO");
        
        try (BatchWriter writer = connector.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, config)) {
            Mutation m = new Mutation(from.toUpperCase());
            m.put("DATAWAVE", to.toUpperCase() + "\u0000forward", viz, blank);
            writer.addMutation(m);
        }
    }
    
    protected void insertReverseModel(String from, String to) throws Throwable {
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        ColumnVisibility viz = new ColumnVisibility("FOO");
        
        try (BatchWriter writer = connector.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, config)) {
            Mutation m = new Mutation(from.toUpperCase());
            m.put("DATAWAVE", to.toUpperCase() + "\u0000reverse", viz, blank);
            writer.addMutation(m);
        }
    }
    
    protected Iterator<DiscoveredThing> runTestQuery(String querystr) throws Throwable {
        return runTestQuery(querystr, dateFormatter.parse("20130101"), dateFormatter.parse("20130102"));
    }
    
    protected Iterator<DiscoveredThing> runTestQuery(String querystr, Date startDate, Date endDate) throws Throwable {
        return runTestQuery(querystr, new HashMap<>(), startDate, endDate);
    }
    
    protected Iterator<DiscoveredThing> runTestQuery(String querystr, Map<String,String> params, Date startDate, Date endDate) throws Throwable {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(queryAuths);
        settings.setQuery(querystr);
        settings.setId(UUID.randomUUID());
        settings.addParameters(params);
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, auths);
        logic.setupQuery(config);
        return logic.iterator();
    }
    
    @Test
    public void testUnfieldedLiterals() throws Throwable {
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("bbc OR onyx"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        assertEquals(ImmutableSet.of(Pair.with("bbc", "NETWORK"), Pair.with("onyx", "POKEMON"), Pair.with("onyx", "ROCK"), Pair.with("onyx", "ROOSTER")),
                        matches);
    }
    
    @Test
    public void testUnfieldedPatterns() throws Throwable {
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("*er OR m*"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        
        assertEquals(ImmutableSet.of(Pair.with("motorcycle", "VEHICLE"), Pair.with("motorboat", "VEHICLE"), Pair.with("police officer", "OTHERPERSON"),
                        Pair.with("skydiver", "OCCUPATION"), Pair.with("rooster", "COCKADOODLEDOO")), matches);
    }
    
    @Test
    public void testUnfielded() throws Throwable {
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("*er OR trophy"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        
        assertEquals(ImmutableSet.of(Pair.with("trophy", "PRIZE"), Pair.with("police officer", "OTHERPERSON"), Pair.with("skydiver", "OCCUPATION"),
                        Pair.with("rooster", "COCKADOODLEDOO")), matches);
    }
    
    @Test
    public void testFieldedLiteral() throws Throwable {
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("rock:onyx OR pokemon:onyx"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        assertEquals(ImmutableSet.of(Pair.with("onyx", "POKEMON"), Pair.with("onyx", "ROCK")), matches);
    }
    
    @Test
    public void testFieldedPattern() throws Throwable {
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("vehicle:*r*k OR bird:*r*k"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        
        assertEquals(ImmutableSet.of(Pair.with("firetruck", "VEHICLE"), Pair.with("ruddy duck", "BIRD")), matches);
    }
    
    @Test
    public void testFielded() throws Throwable {
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("pokemon:onyx OR bird:*r*k"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        
        assertEquals(ImmutableSet.of(Pair.with("onyx", "POKEMON"), Pair.with("ruddy duck", "BIRD")), matches);
    }
    
    @Test
    public void testReverse() throws Throwable {
        for (Pair p : terms2) {
            insertIndex(p);
        }
        
        Set<Pair<String,String>> matches = Sets.newHashSet();
        for (Iterator<DiscoveredThing> it = runTestQuery("*.sky*er"); it.hasNext();) {
            DiscoveredThing thing = it.next();
            matches.add(Pair.with(thing.getTerm(), thing.getField()));
        }
        
        assertEquals(ImmutableSet.of(Pair.with("xxx.skydiver", "OCCUPATION"), Pair.with("yyy.skydiver", "OCCUPATION")), matches);
    }
    
}
