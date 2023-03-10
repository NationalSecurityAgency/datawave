package datawave.ingest.table.balancer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import datawave.common.test.integration.IntegrationTest;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.master.balancer.GroupBalancer.Location;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShardedTableTabletBalancerTest {
    private static final TableId TNAME = TableId.of("s");
    
    private TestTServers testTServers;
    private TestShardedTableTabletBalancer testBalancer;
    private long randomSeed = new Random().nextLong();
    
    @Rule
    public TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            System.err.println("Random seed for this test was: " + randomSeed);
        }
    };
    
    @Before
    public void setUp() throws Exception {
        testTServers = new TestTServers(new Random(randomSeed));
        testBalancer = new TestShardedTableTabletBalancer(testTServers);
    }
    
    @Test
    public void testMaxMigrations() {
        assertEquals(ShardedTableTabletBalancer.MAX_MIGRATIONS_DEFAULT, testBalancer.getMaxMigrations());
    }
    
    @Test
    public void testGetAssignments() {
        testTServers.addTServers("127.0.0.1", "127.0.0.1", "127.0.0.1");
        
        // Two extents per server, but no overlap of any given day.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        unassigned.put(makeExtent(TNAME, "20100123_1", null), null);
        unassigned.put(makeExtent(TNAME, "20100123_2", "20100123_1"), null);
        unassigned.put(makeExtent(TNAME, "20100123_3", "20100123_2"), null);
        unassigned.put(makeExtent(TNAME, "20100124_1", "20100123_3"), null);
        unassigned.put(makeExtent(TNAME, "20100124_2", "20100124_1"), null);
        unassigned.put(makeExtent(TNAME, "20100124_3", "20100124_2"), null);
        
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        
        // Apply the assignments and make sure we're balanced.
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        testTServers.checkBalance(testBalancer.getPartitioner());
        
        // Add a new server and assignment
        testTServers.addTServer("127.0.0.1");
        unassigned.clear();
        unassigned.put(makeExtent(TNAME, "20100124_4", "20100124_3"), null);
        
        assignments.clear();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        
        // Run the balancer. It should balance once after the additional assignment, and then everything should be balanced.
        runAndCheckBalance(1);
    }
    
    @Test
    public void testOverloadedAssignments() {
        testTServers.addTServers("127.0.0.1", "127.0.0.1", "127.0.0.1");
        
        // Double the number of extents as tablet servers. Two should go on each.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        unassigned.put(makeExtent(TNAME, "20100123_1", null), null);
        unassigned.put(makeExtent(TNAME, "20100123_2", "20100123_1"), null);
        unassigned.put(makeExtent(TNAME, "20100123_3", "20100123_2"), null);
        unassigned.put(makeExtent(TNAME, "20100123_4", "20100123_3"), null);
        unassigned.put(makeExtent(TNAME, "20100123_5", "20100123_4"), null);
        unassigned.put(makeExtent(TNAME, "20100123_6", "20100123_5"), null);
        
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        
        // Apply the assignments and make sure we're balanced.
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        testTServers.checkBalance(testBalancer.getPartitioner());
    }
    
    @Test
    public void testBalanceShardDays() {
        
        TServerInstance tsi;
        
        tsi = testTServers.addTServer("127.0.0.1");
        testTServers.addTablet(makeExtent(TNAME, "20100123_1", null), tsi);
        testTServers.addTablet(makeExtent(TNAME, "20100123_2", "20100123_1"), tsi);
        
        tsi = testTServers.addTServer("127.0.0.1");
        testTServers.addTablet(makeExtent(TNAME, "20100124_1", "20100123_2"), tsi);
        testTServers.addTablet(makeExtent(TNAME, "20100124_2", "20100124_1"), tsi);
        
        runAndCheckBalance(1);
    }
    
    @Test
    public void testRecognizeSplit() {
        testTServers.addTServer("127.0.0.1");
        TServerInstance server2 = testTServers.addTServer("127.0.0.1");
        
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        unassigned.put(makeExtent(TNAME, "20100123_1", null), null);
        unassigned.put(makeExtent(TNAME, "20100123_2", "20100123_1"), null);
        unassigned.put(makeExtent(TNAME, "20100124_1", "20100123_2"), null);
        unassigned.put(makeExtent(TNAME, "20100124_2", "20100124_1"), null);
        
        // Make the initial assignments. There should be one piece of each day on each server
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        testTServers.checkBalance(testBalancer.getPartitioner());
        
        // Fake a split of the last shard to add two more extents.
        testTServers.addTablet(makeExtent(TNAME, "20100124_3", "20100124_2"), server2);
        testTServers.addTablet(makeExtent(TNAME, "20100124_4", "20100124_3"), server2);
        
        // Call balance and ensure that one of the new tablets gets moved to the other server
        // Also ensure that we picked up the split and accounted for it.
        runAndCheckBalance(1);
    }
    
    @Test
    public void testRebalanceFromOneServer() {
        testTServers.addTServer("127.0.0.1");
        
        // In the end, we want 3 tablet servers with one piece of each day on each.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        unassigned.put(makeExtent(TNAME, "20100123_1", null), null);
        unassigned.put(makeExtent(TNAME, "20100123_2", "20100123_1"), null);
        unassigned.put(makeExtent(TNAME, "20100123_3", "20100123_2"), null);
        unassigned.put(makeExtent(TNAME, "20100124_1", "20100123_3"), null);
        unassigned.put(makeExtent(TNAME, "20100124_2", "20100124_1"), null);
        unassigned.put(makeExtent(TNAME, "20100124_3", "20100124_2"), null);
        
        // Do initial assignments. Everything will get assigned to one tablet server.
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        assertEquals(6, assignments.size());
        testTServers.applyAssignments(assignments);
        testTServers.checkBalance(testBalancer.getPartitioner());
        
        // Now bring a second tablet server online, and rebalance.
        testTServers.addTServer("127.0.0.1");
        runAndCheckBalance(1);
        
        // Now bring the third server online, rebalance, and check that we are balanced.
        testTServers.addTServer("127.0.0.1");
        runAndCheckBalance(1);
        testTServers.checkDateDistribution();
    }
    
    @Test
    public void testFlipFlopBalance() {
        TServerInstance server1 = testTServers.addTServer("127.0.0.1");
        TServerInstance server2 = testTServers.addTServer("127.0.0.1");
        
        // Make assignments (all shards on a single server)
        testTServers.addTablet(makeExtent(TNAME, "20100123_1", null), server1);
        testTServers.addTablet(makeExtent(TNAME, "20100123_2", "20100123_1"), server1);
        testTServers.addTablet(makeExtent(TNAME, "20100124_1", "20100123_2"), server2);
        testTServers.addTablet(makeExtent(TNAME, "20100124_2", "20100124_1"), server2);
        
        // Now balance and make sure we flip-flop
        runAndCheckBalance(1);
        testTServers.checkDateDistribution();
    }
    
    @Test
    public void testNoRebalanceWithPendingMigrationsForServer() {
        testTServers.addTServer("127.0.0.1");
        
        // In the end, we want 3 tablet servers with one piece of each day on each.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        unassigned.put(makeExtent(TNAME, "20100123_1", null), null);
        unassigned.put(makeExtent(TNAME, "20100123_2", "20100123_1"), null);
        unassigned.put(makeExtent(TNAME, "20100123_3", "20100123_2"), null);
        unassigned.put(makeExtent(TNAME, "20100124_1", "20100123_3"), null);
        unassigned.put(makeExtent(TNAME, "20100124_2", "20100124_1"), null);
        unassigned.put(makeExtent(TNAME, "20100124_3", "20100124_2"), null);
        
        // Do initial assignments. Everything will get assigned to one tablet server.
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        assertEquals(6, assignments.size());
        testTServers.applyAssignments(assignments);
        testTServers.checkBalance(testBalancer.getPartitioner());
        
        // Now bring a second tablet server online, and rebalance.
        testTServers.addTServer("127.0.0.1");
        runAndCheckBalance(1);
        
        // Now bring the third server online, rebalance, and check that we are balanced.
        testTServers.addTServer("127.0.0.1");
        
        // Balance first with pending migrations w/ our table name in them and make sure no balancing happens
        ArrayList<TabletMigration> migrationsOut = new ArrayList<>();
        TableId foo = TableId.of("foo");
        TableId bar = TableId.of("bar");
        HashSet<KeyExtent> migrations = Sets.newHashSet(new KeyExtent(foo, new Text("2"), new Text("1")), new KeyExtent(bar, new Text("2"), new Text("1")),
                        new KeyExtent(TNAME, new Text("2"), new Text("1")));
        long balanceWaitTime = testBalancer.balance(testTServers.getCurrent(), migrations, migrationsOut);
        assertEquals("Incorrect balance wait time reported", 5000, balanceWaitTime);
        assertTrue("Generated migrations when we had pending migrations for our table! [" + migrationsOut + "]", migrationsOut.isEmpty());
        
        // Now balance with pending migrations w/o our table name and make sure everything balances.
        migrations = Sets.newHashSet(new KeyExtent(foo, new Text("2"), new Text("1")), new KeyExtent(bar, new Text("2"), new Text("1")));
        balanceWaitTime = testBalancer.balance(testTServers.getCurrent(), migrations, migrationsOut);
        assertEquals("Incorrect balance wait time reported", 5000, balanceWaitTime);
        ensureUniqueMigrations(migrationsOut);
        testTServers.applyMigrations(migrationsOut);
        assertEquals(4, migrationsOut.size());
        testTServers.checkBalance(testBalancer.getPartitioner());
    }
    
    @Test
    @Category(IntegrationTest.class)
    public void testRandomPerturbations() {
        final int NUM_TSERVERS = 255;
        final int NUM_SHARDS = 241;
        final int NUM_DAYS = 60;
        for (int i = 0; i < NUM_TSERVERS; ++i) {
            testTServers.addTServer("127.0.0.1");
        }
        
        // Come up with extents for 2 months at 241 shards per day.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        GregorianCalendar cal = new GregorianCalendar();
        String[] shardPartitions = new String[NUM_SHARDS];
        for (int i = 0; i < shardPartitions.length; ++i)
            shardPartitions[i] = "_" + i;
        Arrays.sort(shardPartitions);
        
        cal.set(2010, Calendar.JULY, 1, 0, 0, 0);
        String prevRow = null;
        for (int i = 0; i < NUM_DAYS; i++) {
            String date = fmt.format(cal.getTime());
            for (String shardPartition : shardPartitions) {
                String endRow = date + shardPartition;
                unassigned.put(makeExtent(TNAME, endRow, prevRow), null);
                prevRow = endRow;
            }
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
        
        // Assign the initial extents and make sure they're balanced.
        int totalExtents = unassigned.size();
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        
        assertEquals(totalExtents, assignments.size());
        
        int evenMin = Math.max(NUM_SHARDS / NUM_TSERVERS, 1);
        int evenMax = NUM_SHARDS / NUM_TSERVERS;
        if (NUM_SHARDS % NUM_TSERVERS > 0)
            evenMax++;
        
        // There should be between evenMin and evenMax per day
        testTServers.checkShardsPerDay(evenMin, evenMax);
        testTServers.checkBalance(testBalancer.getPartitioner());
        
        // now lets randomly perturbate the extents and test the balancing
        testTServers.peturbBalance();
        
        runAndCheckBalance(5);
        testTServers.checkShardsPerDay(evenMin, evenMax + 1);
        testTServers.checkDateDistribution();
    }
    
    @Test
    public void testLargeRebalance() {
        // Start out with 255 tablet servers. Later, we'll add more.
        final int NUM_TSERVERS = 255;
        for (int i = 0; i < NUM_TSERVERS; ++i) {
            testTServers.addTServer("127.0.0.1");
        }
        
        // Come up with extents for 2 months at 241 shards per day.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        GregorianCalendar cal = new GregorianCalendar();
        String[] shardPartitions = new String[241];
        for (int i = 0; i < shardPartitions.length; ++i)
            shardPartitions[i] = "_" + i;
        Arrays.sort(shardPartitions);
        
        cal.set(2010, Calendar.JULY, 1, 0, 0, 0);
        String prevRow = null;
        for (int i = 0; i < 60; i++) {
            String date = fmt.format(cal.getTime());
            for (String shardPartition : shardPartitions) {
                String endRow = date + shardPartition;
                unassigned.put(makeExtent(TNAME, endRow, prevRow), null);
                prevRow = endRow;
            }
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
        
        // Assign the initial extents and make sure they're balanced.
        int totalExtents = unassigned.size();
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        
        assertEquals(totalExtents, assignments.size());
        
        int evenMin = Math.max(241 / NUM_TSERVERS, 1);
        int evenMax = 241 / NUM_TSERVERS;
        if (241 % NUM_TSERVERS > 0)
            evenMax++;
        
        testTServers.checkShardsPerDay(evenMin, evenMax);
        
        // Now bring on more servers and ensure it's all balanced.
        for (int i = 255; i < 915; ++i) {
            testTServers.addTServer("127.0.0.1");
        }
        evenMin = Math.max(241 / 915, 1);
        evenMax = 241 / 915;
        if (241 % 915 > 0)
            evenMax++;
        
        // and balance
        runAndCheckBalance(10);
        testTServers.checkShardsPerDay(evenMin, evenMax);
        testTServers.checkDateDistribution();
        
    }
    
    @Test
    public void testLopsidedBalance() {
        // Start out with 255 tablet servers. Later, we'll add more
        final int NUM_TSERVERS = 255;
        TServerInstance tsi = null;
        for (int i = 0; i < NUM_TSERVERS; ++i) {
            tsi = testTServers.addTServer("127.0.0.1");
        }
        
        // Come up with extents for 2 months at 241 shards per day.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        GregorianCalendar cal = new GregorianCalendar();
        String[] shardPartitions = new String[241];
        for (int i = 0; i < shardPartitions.length; ++i)
            shardPartitions[i] = "_" + i;
        Arrays.sort(shardPartitions);
        
        cal.set(2010, Calendar.JULY, 1, 0, 0, 0);
        String prevRow = null;
        for (int i = 0; i < 60; i++) {
            String date = fmt.format(cal.getTime());
            for (String shardPartition : shardPartitions) {
                String endRow = date + shardPartition;
                unassigned.put(makeExtent(TNAME, endRow, prevRow), null);
                prevRow = endRow;
            }
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
        
        int totalExtents = unassigned.size();
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        
        assertEquals(totalExtents, assignments.size());
        
        int evenMin = Math.max(241 / NUM_TSERVERS, 1);
        int evenMax = 241 / NUM_TSERVERS;
        if (241 % NUM_TSERVERS > 0)
            evenMax++;
        
        testTServers.checkShardsPerDay(evenMin, evenMax);
        
        // now create an addition day of shards
        String date = fmt.format(cal.getTime());
        cal.add(Calendar.DAY_OF_YEAR, 1);
        LinkedList<KeyExtent> dayExtents = new LinkedList<>();
        for (String shardPartition : shardPartitions) {
            String endRow = date + shardPartition;
            dayExtents.add(makeExtent(TNAME, endRow, prevRow));
            prevRow = endRow;
        }
        
        // add all save one of the key extents to one of the servers
        while (dayExtents.size() > 1) {
            testTServers.addTablet(dayExtents.pop(), tsi);
        }
        
        // bring one more server on line and assign it one of the shards from the new day
        testTServers.addTablet(dayExtents.pop(), "127.0.0.1");
        
        // and balance
        runAndCheckBalance(2);
        testTServers.checkDateDistribution();
    }
    
    @Test
    @Category(IntegrationTest.class)
    public void testHugeBalance() {
        int NUM_TSERVERS = 4000;
        int NUM_SHARDS = 317;
        int NUM_DAYS = 365 * 2;
        
        for (int i = 0; i < NUM_TSERVERS; ++i) {
            testTServers.addTServer("127.0.0.1");
        }
        
        // Come up with extents for NUM_DAYS days at NUM_SHARDS shards per day.
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<>();
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        GregorianCalendar cal = new GregorianCalendar();
        String[] shardPartitions = new String[NUM_SHARDS];
        for (int i = 0; i < shardPartitions.length; ++i)
            shardPartitions[i] = "_" + i;
        Arrays.sort(shardPartitions);
        
        cal.set(2010, Calendar.JANUARY, 1, 0, 0, 0);
        String prevRow = null;
        for (int i = 0; i < NUM_DAYS; i++) {
            String date = fmt.format(cal.getTime());
            for (String shardPartition : shardPartitions) {
                String endRow = date + shardPartition;
                unassigned.put(makeExtent(TNAME, endRow, prevRow), null);
                prevRow = endRow;
            }
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
        
        // Assign the initial extents and make sure they're balanced.
        int totalExtents = unassigned.size();
        Map<KeyExtent,TServerInstance> assignments = new HashMap<>();
        testBalancer.getAssignments(testTServers.getCurrent(), unassigned, assignments);
        testTServers.applyAssignments(assignments);
        
        assertEquals(totalExtents, assignments.size());
        
        int evenMin = Math.max(NUM_SHARDS / NUM_TSERVERS, 1);
        int evenMax = NUM_SHARDS / NUM_TSERVERS;
        if (NUM_SHARDS % NUM_TSERVERS > 0)
            evenMax++;
        
        // There should be between evenMin and evenMax per day
        testTServers.checkShardsPerDay(evenMin, evenMax);
        testTServers.checkBalance(testBalancer.getPartitioner());
        
        // now lets randomly perturbate the extents and test the balancing
        testTServers.peturbBalance();
        
        runAndCheckBalance(5);
        testTServers.checkShardsPerDay(evenMin, evenMax);
        testTServers.checkDateDistribution();
    }
    
    @Test
    public void testSingleTablet() {
        
        TServerInstance tsi;
        
        tsi = testTServers.addTServer("127.0.0.1");
        testTServers.addTablet(makeExtent(TNAME, null, null), tsi);
        
        runAndCheckBalance(1);
    }
    
    @Test
    public void testInvalidDateFormat() {
        
        TServerInstance server1 = testTServers.addTServer("127.0.0.1");
        testTServers.addTServer("127.0.0.1");
        
        // Make assignments (all shards on a single server)
        testTServers.addTablet(makeExtent(TNAME, "group1", null), server1);
        testTServers.addTablet(makeExtent(TNAME, "group2", "group1"), server1);
        testTServers.addTablet(makeExtent(TNAME, null, "group2"), server1);
        
        // Now balance
        runAndCheckBalance(1);
    }
    
    private void runAndCheckBalance(int numPasses) {
        
        // Balance the number of times we're told to
        ArrayList<TabletMigration> migrationsOut = new ArrayList<>();
        for (int i = 1; i <= numPasses; i++) {
            migrationsOut.clear();
            testBalancer.balance(testTServers.getCurrent(), new HashSet<>(), migrationsOut);
            ensureUniqueMigrations(migrationsOut);
            testTServers.applyMigrations(migrationsOut);
            
            if (migrationsOut.isEmpty())
                break;
        }
        // Then balance one more time to make sure no migrations are returned.
        migrationsOut.clear();
        testBalancer.balance(testTServers.getCurrent(), new HashSet<>(), migrationsOut);
        assertEquals("Left with " + migrationsOut.size() + " migrations after " + numPasses + " balance attempts.", 0, migrationsOut.size());
        testTServers.checkBalance(testBalancer.getPartitioner());
    }
    
    private static KeyExtent makeExtent(TableId table, String end, String prev) {
        return new KeyExtent(table, toText(end), toText(prev));
    }
    
    private static Text toText(String value) {
        if (value != null)
            return new Text(value);
        return null;
    }
    
    private void ensureUniqueMigrations(ArrayList<TabletMigration> migrations) {
        Set<KeyExtent> migrated = new HashSet<>();
        for (TabletMigration m : migrations) {
            assertFalse("Found multiple migrations for the same tablet: " + m.tablet, migrated.contains(m.tablet));
            migrated.add(m.tablet);
        }
    }
    
    private static class TestTServers {
        private final Set<TServerInstance> tservers = new HashSet<>();
        private final SortedMap<KeyExtent,TServerInstance> tabletLocs = new TreeMap<>();
        private int portNumber = 1000;
        private Random random;
        
        public TestTServers(Random random) {
            this.random = random;
        }
        
        public void addTServers(String... locations) {
            for (String location : locations) {
                addTServer(location);
            }
        }
        
        public TServerInstance addTServer(String location) {
            return addTServer(location, portNumber++);
        }
        
        public TServerInstance addTServer(String location, int port) {
            TServerInstance tsi = new TServerInstance(location + ":" + port, 6);
            tservers.add(tsi);
            return tsi;
        }
        
        public void addTablet(KeyExtent extent, String location) {
            TServerInstance tsi = null;
            for (TServerInstance candidate : tservers) {
                if (candidate.getLocation().toString().equals(location)) {
                    tsi = candidate;
                    break;
                }
            }
            if (tsi == null)
                tsi = addTServer(location);
            addTablet(extent, tsi);
        }
        
        public void addTablet(KeyExtent extent, TServerInstance tsi) {
            tabletLocs.put(extent, tsi);
        }
        
        public void applyAssignments(Map<KeyExtent,TServerInstance> assignments) {
            for (Entry<KeyExtent,TServerInstance> entry : assignments.entrySet()) {
                KeyExtent extentToAssign = entry.getKey();
                TServerInstance assignedServer = entry.getValue();
                assertTrue("Assignments list has server instance " + entry.getValue() + " that isn't in our servers list.", tservers.contains(assignedServer));
                tabletLocs.put(extentToAssign, assignedServer);
            }
        }
        
        public void applyMigrations(List<TabletMigration> migrationsOut) {
            for (TabletMigration migration : migrationsOut) {
                tabletLocs.put(migration.tablet, migration.newServer);
            }
        }
        
        public void checkBalance(Function<KeyExtent,String> partitioner) {
            checkPartitioning(partitioner);
            
            MapCounter<String> groupCounts = new MapCounter<>();
            Map<TServerInstance,MapCounter<String>> tserverGroupCounts = new HashMap<>(tservers.size());
            
            for (Entry<KeyExtent,TServerInstance> entry : tabletLocs.entrySet()) {
                String group = partitioner.apply(entry.getKey());
                TServerInstance loc = entry.getValue();
                
                groupCounts.increment(group, 1);
                MapCounter<String> tgc = tserverGroupCounts.get(loc);
                if (tgc == null) {
                    tgc = new MapCounter<>();
                    tserverGroupCounts.put(loc, tgc);
                }
                
                tgc.increment(group, 1);
            }
            
            Map<String,Integer> expectedCounts = new HashMap<>();
            
            int totalExtra = 0;
            for (String group : groupCounts.keySet()) {
                long groupCount = groupCounts.get(group);
                totalExtra += groupCount % tservers.size();
                expectedCounts.put(group, (int) (groupCount / tservers.size()));
            }
            
            // The number of extra tablets from all groups that each tserver must have.
            int expectedExtra = totalExtra / tservers.size();
            int maxExtraGroups = expectedExtra + ((totalExtra % tservers.size() > 0) ? 1 : 0);
            
            for (Entry<TServerInstance,MapCounter<String>> entry : tserverGroupCounts.entrySet()) {
                MapCounter<String> tgc = entry.getValue();
                int tserverExtra = 0;
                for (String group : groupCounts.keySet()) {
                    assertTrue("Group " + group + " had " + tgc.get(group) + " tablets on " + entry.getKey() + ", which is less than the expected minimum of "
                                    + expectedCounts.get(group), tgc.get(group) >= expectedCounts.get(group));
                    assertTrue("Group " + group + " had " + tgc.get(group) + " tablets on " + entry.getKey()
                                    + ", which is greater than the expected maximum of " + (expectedCounts.get(group) + 1),
                                    tgc.get(group) <= expectedCounts.get(group) + 1);
                    tserverExtra += tgc.get(group) - expectedCounts.get(group);
                }
                
                assertTrue("tserverExtra of " + tserverExtra + " is less than expected " + expectedExtra, tserverExtra >= expectedExtra);
                assertTrue("tserverExtra of " + tserverExtra + " is greater than expected " + maxExtraGroups, tserverExtra <= maxExtraGroups);
            }
        }
        
        public void checkPartitioning(Function<KeyExtent,String> partitioner) {
            Map<String,String> partitions = new HashMap<>();
            for (Entry<KeyExtent,TServerInstance> entry : tabletLocs.entrySet()) {
                KeyExtent extent = entry.getKey();
                String date = "null";
                if (extent.getEndRow() != null) {
                    String er = extent.getEndRow().toString();
                    int idx = er.length() >= 8 ? 8 : er.length();
                    date = er.substring(0, idx);
                }
                String groupID = partitioner.apply(extent);
                
                if (!partitions.containsKey(date))
                    partitions.put(date, groupID);
                
                assertEquals("Extent " + extent + " is assigned to partition " + groupID + " but we expected " + partitions.get(date), groupID,
                                partitions.get(date));
            }
        }
        
        public void checkDateDistribution() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            
            // Accumulate tservers per day, per week, per month
            HashMap<LocalDate,Set<TServerInstance>> dayCounts = new HashMap<>();
            HashMap<Integer,Set<TServerInstance>> weekCounts = new HashMap<>();
            HashMap<Integer,Set<TServerInstance>> monthCounts = new HashMap<>();
            MapCounter<LocalDate> shardsPerDay = new MapCounter<>();
            MapCounter<Integer> shardsPerWeek = new MapCounter<>();
            MapCounter<Integer> shardsPerMonth = new MapCounter<>();
            for (Entry<KeyExtent,TServerInstance> entry : tabletLocs.entrySet()) {
                String ds = entry.getKey().getEndRow().toString().substring(0, 8);
                LocalDate date = LocalDate.parse(ds, formatter);
                
                Set<TServerInstance> set;
                
                set = dayCounts.computeIfAbsent(date, k -> new HashSet<>());
                set.add(entry.getValue());
                shardsPerDay.increment(date, 1);
                
                int week = date.get(WeekFields.ISO.weekOfWeekBasedYear());
                set = weekCounts.computeIfAbsent(week, k -> new HashSet<>());
                set.add(entry.getValue());
                shardsPerWeek.increment(week, 1);
                
                int month = date.getMonthValue();
                set = monthCounts.computeIfAbsent(month, k -> new HashSet<>());
                set.add(entry.getValue());
                shardsPerMonth.increment(month, 1);
            }
            
            for (Integer month : monthCounts.keySet()) {
                int count = monthCounts.get(month).size();
                int shardsInMonth = (int) shardsPerMonth.get(month);
                if (shardsInMonth < tservers.size()) {
                    float percent = count / (float) shardsInMonth;
                    assertTrue("Month " + month + " only has tablets on " + (percent * 100) + "% of the tservers per shard in month (" + count + "/"
                                    + shardsInMonth + ")", percent >= 0.9f);
                } else {
                    float percent = count / (float) tservers.size();
                    assertTrue("Month " + month + " only has tablets on " + (percent * 100) + "% of the tservers (" + count + "/" + tservers.size() + ")",
                                    percent >= 0.9f);
                }
            }
            
            for (Integer week : weekCounts.keySet()) {
                int count = weekCounts.get(week).size();
                int shardsInWeek = (int) shardsPerWeek.get(week);
                if (shardsInWeek < tservers.size()) {
                    float percent = count / (float) shardsInWeek;
                    assertTrue("Week " + week + " only has tablets on " + (percent * 100) + "% of the tservers (" + count + "/" + tservers.size() + ")",
                                    percent >= 0.9f);
                } else {
                    // Given the way we partition data, a week could easily be split across multiple groups and then the two pieces of the week
                    // that span groups might get stacked on the same tservers a little bit more, so we check a lower coverage percentage here.
                    float percent = count / (float) tservers.size();
                    assertTrue("Week " + week + " only has tablets on " + (percent * 100) + "% of the tservers (" + count + "/" + tservers.size() + ")",
                                    percent >= 0.7f);
                }
            }
            
            for (LocalDate date : dayCounts.keySet()) {
                int count = dayCounts.get(date).size();
                int shardsInDay = (int) shardsPerDay.get(date);
                assertTrue(shardsInDay <= tservers.size());
                assertEquals("Expected day " + date + " to be on " + shardsInDay + " tservers, but only found on " + count, shardsInDay, count);
            }
        }
        
        public void checkShardsPerDay(int evenMin, int evenMax) {
            Map<TServerInstance,Multiset<String>> shardsPerServer = new HashMap<>();
            for (Entry<KeyExtent,TServerInstance> entry : tabletLocs.entrySet()) {
                TServerInstance tserver = entry.getValue();
                Multiset<String> set = shardsPerServer.get(tserver);
                if (set == null) {
                    set = HashMultiset.create();
                    shardsPerServer.put(tserver, set);
                }
                String group = entry.getKey().getEndRow().toString().substring(0, 8);
                set.add(group);
            }
            
            for (Entry<TServerInstance,Multiset<String>> entry : shardsPerServer.entrySet()) {
                Multiset<String> entries = entry.getValue();
                for (String date : entries.elementSet()) {
                    assertTrue(entries.count(date) + " less than minimum of " + evenMin, entries.count(date) >= evenMin);
                    assertTrue(entries.count(date) + " greater than minimum of " + evenMax + " for " + date, entries.count(date) <= evenMax);
                }
            }
        }
        
        public void peturbBalance() {
            Multimap<TServerInstance,KeyExtent> serverTablets = HashMultimap.create();
            for (Entry<KeyExtent,TServerInstance> entry : tabletLocs.entrySet()) {
                serverTablets.put(entry.getValue(), entry.getKey());
            }
            
            ArrayList<TServerInstance> serversArray = new ArrayList<>(tservers);
            for (int i = 0; i < 101; i++) {
                // Find a random source server that has at least some tablets assigned to it.
                TServerInstance fromServer = serversArray.get(random.nextInt(serversArray.size()));
                while (!serverTablets.containsKey(fromServer)) {
                    fromServer = serversArray.get(random.nextInt(serversArray.size()));
                }
                // Find a random destination server that's different.
                TServerInstance toServer;
                do {
                    toServer = serversArray.get(random.nextInt(serversArray.size()));
                } while (fromServer.equals(toServer));
                
                ArrayList<KeyExtent> fromExtents = new ArrayList<>(serverTablets.get(fromServer));
                int migrationsToMove = random.nextInt(fromExtents.size());
                for (int j = 0; j < migrationsToMove; j++) {
                    KeyExtent extent = fromExtents.get(random.nextInt(fromExtents.size()));
                    fromExtents.remove(extent); // we have a local copy
                    assertTrue("Couldn't remove extent " + extent + " from server " + fromServer, serverTablets.remove(fromServer, extent));
                    assertTrue("Couldn't add extent " + extent + " to server " + toServer, serverTablets.put(toServer, extent));
                    assertEquals("Extent " + extent + " wasn't assigned to " + fromServer, fromServer, tabletLocs.put(extent, toServer));
                }
            }
        }
        
        public SortedMap<TServerInstance,TabletServerStatus> getCurrent() {
            SortedMap<TServerInstance,TabletServerStatus> current = new TreeMap<>();
            for (TServerInstance tserver : tservers) {
                current.put(tserver, new TabletServerStatus());
            }
            return current;
        }
        
        public Iterable<Pair<KeyExtent,Location>> getLocationProvider() {
            return Iterables.transform(tabletLocs.entrySet(), input -> new Pair<>(input.getKey(), new Location(input.getValue())));
        }
    }
    
    private class TestShardedTableTabletBalancer extends ShardedTableTabletBalancer {
        private TestTServers testTServers;
        
        public TestShardedTableTabletBalancer(TestTServers testTServers) {
            super(TNAME);
            this.testTServers = testTServers;
        }
        
        @Override
        protected Iterable<Pair<KeyExtent,Location>> getRawLocationProvider() {
            return testTServers.getLocationProvider();
        }
        
        // Overridden to make it public
        @Override
        public Function<KeyExtent,String> getPartitioner() {
            return super.getPartitioner();
        }
        
        @Override
        protected long getWaitTime() {
            return 0;
        }
    }
    
}
