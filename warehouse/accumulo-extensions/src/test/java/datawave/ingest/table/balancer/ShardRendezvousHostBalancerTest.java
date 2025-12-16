package datawave.ingest.table.balancer;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import com.google.common.base.Preconditions;

public class ShardRendezvousHostBalancerTest {

    static class TestTServers {
        private final Set<TabletServerId> tservers = new HashSet<>();
        private final SortedMap<TabletId,TabletServerId> tabletLocs = new TreeMap<>();

        public void addTServer(TabletServerId tsi) {
            tservers.add(tsi);
        }

        public void removeTservers(Predicate<TabletServerId> tserverPedicate) {
            tservers.removeIf(tserverPedicate);
            tabletLocs.values().removeIf(tserverPedicate);

        }

        public void applyAssignments(Map<TabletId,TabletServerId> assignments) {
            for (Map.Entry<TabletId,TabletServerId> entry : assignments.entrySet()) {
                TabletId extentToAssign = entry.getKey();
                TabletServerId assignedServer = entry.getValue();
                assertTrue("Assignments list has server instance " + entry.getValue() + " that isn't in our servers list.", tservers.contains(assignedServer));
                tabletLocs.put(extentToAssign, assignedServer);
            }
        }

        public void applyMigrations(List<TabletMigration> migrationsOut) {
            for (TabletMigration migration : migrationsOut) {
                tabletLocs.put(migration.getTablet(), migration.getNewTabletServer());
            }
        }

        public SortedMap<TabletServerId,TServerStatus> getCurrent() {
            SortedMap<TabletServerId,TServerStatus> current = new TreeMap<>();
            for (TabletServerId tserver : tservers) {
                // Currently the balancer only used the key set of this map. Place a strict moock that will fail if the values of the map are used.
                current.put(tserver, EasyMock.strictMock(TServerStatus.class));
            }
            return current;
        }

        public Map<TabletId,TabletServerId> getLocationProvider() {
            return tabletLocs;
        }

        public Map<TabletId,TabletServerId> getUnassigned(Collection<TabletId> allTablets) {
            Map<TabletId,TabletServerId> unassigned = new HashMap<>();
            for (var tid : allTablets) {
                if (tabletLocs.get(tid) == null) {
                    unassigned.put(tid, null);
                }
            }

            return unassigned;
        }

        public void clearLocation(TabletId tablet) {
            tabletLocs.remove(tablet);
        }

        public void clear() {
            tservers.clear();
            tabletLocs.clear();
        }

        public void setLocation(TabletId tablet, TabletServerId tserver) {
            tabletLocs.put(tablet, tserver);
        }
    }

    private AtomicReference<LocalDate> today = new AtomicReference<>();
    private List<TabletId> tablets = new ArrayList<>();
    private final TableId tableId = TableId.of("1");
    private final TestTServers testTServers = new TestTServers();
    private final Map<String,String> tableProps = new TreeMap<>();
    private ShardRendezvousHostBalancer balancer;
    private final Map<TabletId,TabletServerId> aout = new HashMap<>();
    private final List<TabletMigration> migrations = new ArrayList<>();

    @Before
    public void setup() {

        tablets.clear();
        tableProps.clear();
        testTServers.clear();
        aout.clear();
        migrations.clear();

        balancer = new ShardRendezvousHostBalancer(tableId) {
            @Override
            protected LocalDate today() {
                return today.get();
            }

            @Override
            protected long getWaitTime() {
                return 0;
            }
        };

        tableProps.put("table.custom.volume.tier.names", "t1,t2");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", "host0000.*");
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[1-9].*");
        tableProps.put("table.custom.sharded.balancer.max.migrations", "1000");

        balancer.init(new MyBalancerEnvironment(tableId, tablets, testTServers, tableProps));
    }

    public static class TestAssignmentParams implements TabletBalancer.AssignmentParameters {

        private final SortedMap<TabletServerId,TServerStatus> currentStatus;
        private final Map<TabletId,TabletServerId> unassigned;
        private final Map<TabletId,TabletServerId> assignments;

        public TestAssignmentParams(SortedMap<TabletServerId,TServerStatus> currentStatus, Map<TabletId,TabletServerId> unassigned,
                        Map<TabletId,TabletServerId> assignments) {
            this.currentStatus = currentStatus;
            this.unassigned = unassigned;
            this.assignments = assignments;
        }

        @Override
        public SortedMap<TabletServerId,TServerStatus> currentStatus() {
            return currentStatus;
        }

        @Override
        public Map<TabletId,TabletServerId> unassignedTablets() {
            return unassigned;
        }

        @Override
        public void addAssignment(TabletId tabletId, TabletServerId tabletServerId) {
            assignments.put(tabletId, tabletServerId);
        }
    }

    private static class TestBalanceParams implements TabletBalancer.BalanceParameters {

        private final SortedMap<TabletServerId,TServerStatus> currentStatus;
        private final Set<TabletId> migrations;
        private final List<TabletMigration> migrationsOut;

        private TestBalanceParams(SortedMap<TabletServerId,TServerStatus> currentStatus, Set<TabletId> migrations, List<TabletMigration> migrationsOut) {
            this.currentStatus = currentStatus;
            this.migrations = migrations;
            this.migrationsOut = migrationsOut;
        }

        @Override
        public SortedMap<TabletServerId,TServerStatus> currentStatus() {
            return currentStatus;
        }

        @Override
        public Set<TabletId> currentMigrations() {
            return migrations;
        }

        @Override
        public List<TabletMigration> migrationsOut() {
            return migrationsOut;
        }

        @Override
        public String partitionName() {
            return "Null Partition";
        }

        @Override
        public Map<String,TableId> getTablesToBalance() {
            return Map.of();
        }

    }

    @Test
    public void test() throws Exception {

        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);

        // nothing changed so should not migrate anything
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        Assertions.assertEquals(0, migrations.size());

        // move forward by one day, should cause tablets to move
        today.set(parseDay("20010131"));
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        // should migrate all shards in a single day
        Assertions.assertEquals(31, migrations.size());
        testTServers.applyMigrations(migrations);
        // The first set of tservers should loose a day
        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(19, 31, 10, 3);
        // The second set of tserers should gain a day
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(11, 31, 19, 3);
        // should be stable now
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());

        // add two tservers
        generateTabletServers(30, 2, 3).forEach(testTServers::addTServer);
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        testTServers.applyMigrations(migrations);
        // The first set of tservers should stay the same
        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(19, 31, 10, 3);
        // The second set of tservers should balance days across the 2 new tservers
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(11, 31, 21, 3);
        // should be stable now
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());

        // remove a tablet server for case where there are more shards per day than tservers
        var tabletsOnH09 = testTServers.getLocationProvider().values().stream().filter(tabletServerId -> tabletServerId.getHost().equals("host00009")).count();
        assertTrue(tabletsOnH09 > 0);
        testTServers.removeTservers(tabletServerId -> tabletServerId.getHost().equals("host00009"));
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        // balancing will not migrate unassigned tablet, so this should be a noop
        assertTrue(migrations.isEmpty());
        aout.clear();
        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        assertEquals(tabletsOnH09, aout.size());
        testTServers.applyAssignments(aout);
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        // assignment should have put everything in its place such that balancing was not needed
        assertTrue(migrations.isEmpty());
        // The first set of tservers lost a tserver, so should balance tablets across the remaining
        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(19, 31, 9, 3);
        // The second set of tservers should stay the same
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(11, 31, 21, 3);
        // should be stable now
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());

        // remove a tablet server for case where there are more tservers than shards per day
        var tabletsOnH19 = testTServers.getLocationProvider().values().stream().filter(tabletServerId -> tabletServerId.getHost().equals("host00019")).count();
        assertTrue(tabletsOnH19 > 0);
        testTServers.removeTservers(tabletServerId -> tabletServerId.getHost().equals("host00019"));
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        // balancing will not migrate unassigned tablet, so this should be a noop
        assertTrue(migrations.isEmpty());
        aout.clear();
        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        assertEquals(tabletsOnH19, aout.size());
        testTServers.applyAssignments(aout);
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        // assignment should have put everything in its place such that balancing was not needed
        assertTrue(migrations.isEmpty());
        // The first set of tablet servers should stay the same
        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(19, 31, 9, 3);
        // The second set of tservers lost a tserver and should change
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(11, 31, 20, 3);
        // should be stable now
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());
    }

    @Test
    public void testOverlappingServers() throws Exception {
        tableProps.clear();

        // Create tiers w/ overlapping sets of tablet servers
        tableProps.put("table.custom.volume.tier.names", "t1,t2");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", ".*");
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[1-9].*");
        tableProps.put("table.custom.sharded.balancer.max.migrations", "1000");

        balancer.init(new MyBalancerEnvironment(tableId, tablets, testTServers, tableProps));

        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        testTServers.applyAssignments(aout);

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(
                        filter(tabletId -> tabletId.getEndRow().toString().matches("2001011[1-9].*|200101[23].*"), testTServers.getLocationProvider()));
        shardStats.check(20, 31, 29, 3);
        shardStats = ShardStats
                        .compute(filter(tabletId -> tabletId.getEndRow().toString().matches("2001010.*|20010110.*"), testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);
    }

    @Test
    public void testFutureDate() throws Exception {
        tablets.addAll(createShards(tableId, "20010101", 40, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        // The 10 days in the future should go to the tier w/ the lowest days back
        shardStats.check(30, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);
    }

    @Test
    public void testChangingConfig() throws Exception {
        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        assertEquals(1000, balancer.getMaxMigrations());

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);

        // Change the table config, should cause tablets to move around
        tableProps.put("table.custom.volume.tiered.t2.days.back", "10");

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertEquals(310, migrations.size());
        testTServers.applyMigrations(migrations);
        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 19, 3);

        // lower max migrations and cause tablets to move again
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.sharded.balancer.max.migrations", "100");
        for (int i = 0; i < 3; i++) {
            migrations.clear();
            balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
            assertEquals(100, migrations.size());
            testTServers.applyMigrations(migrations);
        }
        migrations.clear();
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertEquals(10, migrations.size());
        testTServers.applyMigrations(migrations);

        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);

        assertEquals(100, balancer.getMaxMigrations());
    }

    @Test
    public void testMissingConfig() throws Exception {

        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        tableProps.clear();

        Assertions.assertThrows(IllegalStateException.class,
                        () -> balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout)));
        Assertions.assertThrows(IllegalStateException.class, () -> balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations)));

        // set the config and it should start working
        tableProps.put("table.custom.volume.tier.names", "t1,t2");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", "host0000.*");
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[1-9].*");
        tableProps.put("table.custom.sharded.balancer.max.migrations", "1000");

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        assertEquals(1000, balancer.getMaxMigrations());

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);
    }

    @Test
    public void testInvalidConfig() throws Exception {

        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        // set an invalid regex
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[1-9).*");
        assertThrows(PatternSyntaxException.class,
                        () -> balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout)));
        assertThrows(PatternSyntaxException.class, () -> balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations)));

        // fix regex and set invalid day
        tableProps.put("table.custom.volume.tiered.t2.days.back", "twenty");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[1-9].*");
        assertThrows(NumberFormatException.class,
                        () -> balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout)));
        assertThrows(NumberFormatException.class, () -> balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations)));

        // fix the config and it should start working
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        assertEquals(1000, balancer.getMaxMigrations());

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);

    }

    @Test
    public void testNoTservers() throws Exception {
        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        assertTrue(aout.isEmpty());
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());
    }

    @Test
    public void testNoTserversForRegex() throws Exception {
        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));

        tableProps.put("table.custom.volume.tier.names", "t1,t2");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", "host0000.*");
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "willNotMatch[12].*");

        generateTabletServers(0, 29, 3).forEach(testTServers::addTServer);

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        // should only assign 20 of the 30 days because tier t2 has no tservers
        shardStats.check(20, 31, 10, 3);
        // should only see the first 10 of 29 host used, no regex matches the other 19 host
        assertTrue(testTServers.getLocationProvider().values().stream().map(TabletServerId::getHost).allMatch(h -> h.matches("host0000.*")));

        assertEquals(1000, balancer.getMaxMigrations());

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.isEmpty());
    }

    @Test
    public void testLast() throws Exception {
        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        var tservers = generateTabletServers(0, 29, 3);
        tservers.forEach(testTServers::addTServer);

        var tablet20010103 = tablets.stream().filter(tabletId -> tabletId.getEndRow().toString().startsWith("20010103")).findFirst().orElseThrow();
        var tablet20010105 = tablets.stream().filter(tabletId -> tabletId.getEndRow().toString().startsWith("20010105")).findFirst().orElseThrow();
        var tablet20010114 = tablets.stream().filter(tabletId -> tabletId.getEndRow().toString().startsWith("20010114")).findFirst().orElseThrow();
        var tablet20010127 = tablets.stream().filter(tabletId -> tabletId.getEndRow().toString().startsWith("20010127")).findFirst().orElseThrow();

        var unassigned = testTServers.getUnassigned(tablets);

        var tserver0003 = tservers.stream().filter(tserver -> tserver.getHost().equals("host00003")).findFirst().orElseThrow();
        var tserver0006 = tservers.stream().filter(tserver -> tserver.getHost().equals("host00006")).findFirst().orElseThrow();
        var tserver0013 = tservers.stream().filter(tserver -> tserver.getHost().equals("host00013")).findFirst().orElseThrow();
        var tserver0025 = tservers.stream().filter(tserver -> tserver.getHost().equals("host00025")).findFirst().orElseThrow();

        // set some last locations
        unassigned.put(tablet20010103, tserver0003); // should not be assigned here
        unassigned.put(tablet20010105, tserver0013); // should be assigned here
        unassigned.put(tablet20010114, tserver0006); // should be assigned here
        unassigned.put(tablet20010127, tserver0025); // should not be assigned here

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), unassigned, aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[1-9].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);

        assertNotEquals(tserver0003, testTServers.getLocationProvider().get(tablet20010103));
        assertEquals(tserver0013, testTServers.getLocationProvider().get(tablet20010105));
        assertEquals(tserver0006, testTServers.getLocationProvider().get(tablet20010114));
        assertNotEquals(tserver0025, testTServers.getLocationProvider().get(tablet20010127));
    }

    @Test
    public void testInvalidShardRow() throws Exception {
        tablets.addAll(createShards(tableId, "20010101", 30, 31));
        today.set(parseDay("20010130"));
        generateTabletServers(0, 29, 3).forEach(testTServers::addTServer);
        generateTabletServers(30, 3, 1).forEach(testTServers::addTServer);

        // Configure a tier for really old data
        tableProps.put("table.custom.volume.tier.names", "t1,t2,t3");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", "host0000.*");
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[12].*");
        tableProps.put("table.custom.volume.tiered.t3.days.back", "6000");
        tableProps.put("table.custom.volume.tiered.t3.tservers", "host0003.*");
        tableProps.put("table.custom.sharded.balancer.max.migrations", "1000");

        // make some invalid tablets
        var tablet1 = makeTablet(tableId, "2001a_09");
        tablets.add(tablet1);
        tablets.add(makeTablet(tableId, "2001a_90"));
        tablets.add(makeTablet(tableId, "2001a_33"));
        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[12].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);
        // The three bad tablets should all group into the tier for really old data
        shardStats = ShardStats.compute(filter("host0003.*", testTServers.getLocationProvider()));
        shardStats.check(1, 3, 3, 1);

        // move one of the bad tablets to a tserver that is not designated for bad tablets
        var tserver1 = testTServers.tservers.stream().filter(tabletServerId -> tabletServerId.getHost().matches("host0000.*")).findFirst().orElseThrow();
        testTServers.setLocation(tablet1, tserver1);

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertEquals(Set.of(tserver1.getHost()),
                        migrations.stream().map(TabletMigration::getOldTabletServer).map(TabletServerId::getHost).collect(Collectors.toSet()));
        assertEquals(Set.of(tablet1), migrations.stream().map(TabletMigration::getTablet).collect(Collectors.toSet()));
        testTServers.applyMigrations(migrations);

        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[12].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);
        shardStats = ShardStats.compute(filter("host0003.*", testTServers.getLocationProvider()));
        shardStats.check(1, 3, 3, 1);

        // add another bad tablet
        tablets.add(makeTablet(tableId, "2001$#%^&@$0101_33"));
        aout.clear();
        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 31, 10, 3);
        shardStats = ShardStats.compute(filter("host000[12].*", testTServers.getLocationProvider()));
        shardStats.check(10, 31, 19, 3);
        // should see one more shard for the epoch day
        shardStats = ShardStats.compute(filter("host0003.*", testTServers.getLocationProvider()));
        shardStats.check(1, 4, 3, 1);

    }

    @Test
    public void testHostWithVaryingTserverCount() throws Exception {
        generateTabletServers(0, 7, 3).forEach(testTServers::addTServer);
        // Create a few that differs from the norm of 3 tservers per host
        generateTabletServers(10, 2, 1).forEach(testTServers::addTServer);
        generateTabletServers(12, 5, 2).forEach(testTServers::addTServer);
        generateTabletServers(17, 1, 4).forEach(testTServers::addTServer);

        generateTabletServers(20, 11, 4).forEach(testTServers::addTServer);
        // For the 2nd group of tservers, create a few that differs from the norm of 4 tservers per host
        generateTabletServers(40, 5, 2).forEach(testTServers::addTServer);

        tablets.addAll(createShards(tableId, "20010101", 31, 31));
        today.set(parseDay("20010131"));

        tableProps.clear();
        tableProps.put("table.custom.volume.tier.names", "t1,t2");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", "host000[01].*");
        tableProps.put("table.custom.volume.tiered.t2.days.back", "20");
        tableProps.put("table.custom.volume.tiered.t2.tservers", "host000[2345].*");

        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        /*
         * There are 37 tservers in tier t1.
         *
         * There are 7 host with 3 tservers, these make up 7*3/37=.5676 of all tservers. For each day w/ 31 tablets they will get round(.5676*31)=18 tablets.
         * That is why shards per day is set to 18 below. The 2 host w/ 1 tservers will get round(2/37*31)=2 of the 31 shards for each day. The 5 host w/ 2
         * tservers will get round(5*2/37*31)=8 of the 31 shards for each day. The 1 host w/ 4 tservers will get round(4/37*31)=3 of the 31 shards for each day.
         */
        var shardStats = ShardStats.compute(filter("host0000.*", testTServers.getLocationProvider()));
        shardStats.check(20, 18, 7, 3);
        shardStats = ShardStats.compute(filter("host0001[01].*", testTServers.getLocationProvider()));
        shardStats.check(20, 2, 2, 1);
        shardStats = ShardStats.compute(filter("host0001[23456].*", testTServers.getLocationProvider()));
        shardStats.check(20, 8, 5, 2);
        shardStats = ShardStats.compute(filter("host00017.*", testTServers.getLocationProvider()));
        shardStats.check(20, 3, 1, 4);

        /*
         * There are a total of 54 tservers in tier t2. The 11 host w/ 4 tservers will get round(11*4/54*31)=25 of the 31 shards per day. The 5 host w/ 2
         * tsevers will get round(5*2/54*31)=6 of the 31 shards per day
         */
        shardStats = ShardStats.compute(filter("host000[23].*", testTServers.getLocationProvider()));
        shardStats.check(11, 25, 11, 4);
        shardStats = ShardStats.compute(filter("host0004.*", testTServers.getLocationProvider()));
        shardStats.check(11, 6, 5, 2);

        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertEquals(0, migrations.size());

        // add a few tablet servers
        generateTabletServers(50, 1, 3).forEach(testTServers::addTServer);
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        assertTrue(migrations.stream().map(tm -> tm.getNewTabletServer().getHost()).noneMatch(h -> h.matches("host000[01].*")));
        testTServers.applyMigrations(migrations);
        shardStats = ShardStats.compute(filter("host000[23].*", testTServers.getLocationProvider()));
        shardStats.check(11, 24, 11, 4);
        shardStats = ShardStats.compute(filter("host0004.*", testTServers.getLocationProvider()));
        shardStats.check(11, 5, 5, 2);
        shardStats = ShardStats.compute(filter("host0005.*", testTServers.getLocationProvider()));
        shardStats.check(11, 2, 1, 3);
    }

    @Test
    public void testHostWithVaryingTserverCountZero() throws Exception {
        tablets.addAll(createShards(tableId, "20000101", 395, 31));
        today.set(parseDay("20010130"));
        generateTabletServers(0, 100, 5).forEach(testTServers::addTServer);
        generateTabletServers(200, 2, 1).forEach(testTServers::addTServer);

        tableProps.clear();
        tableProps.put("table.custom.volume.tier.names", "t1");
        tableProps.put("table.custom.volume.tiered.t1.days.back", "0");
        tableProps.put("table.custom.volume.tiered.t1.tservers", "host.*");

        // There are 502 total tserver. The two host with one tserver should have round(2/502*31)=0 tablets from the 31 shards per day assigned to them. All
        // tablets should go to the 100 host with 5 tservers.
        balancer.getAssignments(new TestAssignmentParams(testTServers.getCurrent(), testTServers.getUnassigned(tablets), aout));
        testTServers.applyAssignments(aout);

        var shardStats = ShardStats.compute(filter("host000.*", testTServers.getLocationProvider()));
        shardStats.check(395, 31, 100, 5);

        String regex = "host002.*";
        // nothing should have been assigned to the two host with a single tserver.
        assertEquals(Map.of(), filter(regex, testTServers.getLocationProvider()));
        // check the regex used above
        assertEquals(2, testTServers.getCurrent().keySet().stream().filter(tabletServerId -> tabletServerId.getHost().matches(regex)).count());
        assertEquals(502, testTServers.getCurrent().keySet().size());

        // Add 98 host with 1 tserver. For the 31 shards per day the should now partition as round(500/600*31)=26 and round(100/600*31)=5.
        generateTabletServers(202, 98, 1).forEach(testTServers::addTServer);
        balancer.balance(new TestBalanceParams(testTServers.getCurrent(), Set.of(), migrations));
        testTServers.applyMigrations(migrations);
        shardStats = ShardStats.compute(filter("host000.*", testTServers.getLocationProvider()));
        shardStats.check(395, 26, 100, 5);
        shardStats = ShardStats.compute(filter("host002.*", testTServers.getLocationProvider()));
        shardStats.check(395, 5, 100, 1);

    }

    private static String getDay(TabletId tabletId) {
        return ShardRendezvousHostBalancer.PARTITIONER.apply(tabletId);
    }

    private static LocalDate parseDay(String day) {
        return ShardRendezvousHostBalancer.parse(day);
    }

    private static Map<TabletId,TabletServerId> filter(String regex, Map<TabletId,TabletServerId> locations) {
        var copy = new HashMap<>(locations);
        copy.values().removeIf(tserver -> !tserver.getHost().matches(regex));
        return copy;
    }

    private static Map<TabletId,TabletServerId> filter(Predicate<TabletId> tabletIdPredicate, Map<TabletId,TabletServerId> locations) {
        var copy = new HashMap<>(locations);
        copy.keySet().removeIf(tabletId -> !tabletIdPredicate.test(tabletId));
        return copy;
    }

    private static class ShardStats {

        final LongSummaryStatistics dayStats;
        final LongSummaryStatistics hostStats;
        final LongSummaryStatistics tserverStats;
        final LongSummaryStatistics hgStats;

        public ShardStats(LongSummaryStatistics dayStats, LongSummaryStatistics hostStats, LongSummaryStatistics tserverStats, LongSummaryStatistics hgStats) {
            this.dayStats = dayStats;
            this.hostStats = hostStats;
            this.tserverStats = tserverStats;
            this.hgStats = hgStats;
        }

        static ShardStats compute(Map<TabletId,TabletServerId> assignedLocations) {
            Map<String,Long> hostGroupCounts = new HashMap<>();
            Map<String,Long> perDayCounts = new HashMap<>();
            Map<String,Long> perHostCounts = new HashMap<>();
            Map<String,Long> perTserverCounts = new HashMap<>();

            var formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

            assignedLocations.forEach((tabletId, tabletServerId) -> {
                var day = parseDay(getDay(tabletId)).format(formatter);
                String host = tabletServerId.getHost();
                hostGroupCounts.merge(host + " " + day, 1L, Long::sum);
                perDayCounts.merge(day, 1L, Long::sum);
                perHostCounts.merge(host, 1L, Long::sum);
                perTserverCounts.merge(tabletServerId.getHost() + ":" + tabletServerId.getPort(), 1L, Long::sum);
            });

            var hgStats = hostGroupCounts.values().stream().mapToLong(l -> l).summaryStatistics();
            var dayStats = perDayCounts.values().stream().mapToLong(l -> l).summaryStatistics();
            var hostStats = perHostCounts.values().stream().mapToLong(l -> l).summaryStatistics();
            var tserverStats = perTserverCounts.values().stream().mapToLong(l -> l).summaryStatistics();

            return new ShardStats(dayStats, hostStats, tserverStats, hgStats);
        }

        void check(long days, long shardsPerDay, long hosts, long tabletServersPerHost) {
            long mingHostGroupCount;
            long maxHostGroupCount;
            if (shardsPerDay < hosts) {
                // This special case exists because the hgStats map does not store zeros for host+day pairs that do not exist in the data. This throws the
                // general calculation off.
                mingHostGroupCount = 1;
                maxHostGroupCount = 1;
            } else {
                mingHostGroupCount = shardsPerDay / hosts;
                maxHostGroupCount = mingHostGroupCount + (shardsPerDay % hosts > 0 ? 1 : 0);
            }

            assertEquals(days, dayStats.getCount(), dayStats::toString);
            assertEquals(hosts, hostStats.getCount(), hostStats::toString);
            assertEquals(hosts * tabletServersPerHost, tserverStats.getCount(), tserverStats::toString);

            // ensure hosts do not have too many or too little days
            assertEquals(mingHostGroupCount, hgStats.getMin(), hgStats::toString);
            assertEquals(maxHostGroupCount, hgStats.getMax(), hgStats::toString);

            // every tablet for a day should be assigned
            assertEquals(shardsPerDay, dayStats.getMin(), dayStats::toString);
            assertEquals(shardsPerDay, dayStats.getMax(), dayStats::toString);

            // ensure tablets spread evenly across hosts
            double totalTablets = days * shardsPerDay;
            assertEquals(totalTablets, dayStats.getSum(), dayStats::toString);
            assertEquals(totalTablets, hostStats.getSum(), hostStats::toString);
            assertEquals(totalTablets, hgStats.getSum(), hgStats::toString);
            assertEquals(totalTablets, tserverStats.getSum(), tserverStats::toString);
        }
    }

    private static List<TabletServerId> generateTabletServers(int startHost, int numHosts, int tserversPerHosts) {
        var all = new ArrayList<TabletServerId>();
        for (int h = 0; h < numHosts; h++) {
            String host = String.format("host%05d", startHost + h);

            for (int p = 0; p < tserversPerHosts; p++) {
                all.add(new TabletServerIdImpl(host, p + 9997, "1234"));
            }
        }
        return all;
    }

    private static List<TabletId> createShards(TableId tableId, String startDate, int days, int shardsPerDay) throws Exception {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        var date = fmt.parse(startDate);
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);

        var shards = new ArrayList<TabletId>(days * shardsPerDay);

        for (int d = 0; d < days; d++) {
            var prefix = fmt.format(cal.getTime());
            for (int s = 0; s < shardsPerDay; s++) {
                shards.add(makeTablet(tableId, prefix + "_" + s));
            }
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }

        return shards;
    }

    private static TabletId makeTablet(TableId tableId, String endRow) {
        return new TabletIdImpl(new KeyExtent(tableId, endRow == null ? null : new Text(endRow), null));
    }

    private static class MyBalancerEnvironment implements BalancerEnvironment {
        private final TableId tableId;
        private final List<TabletId> tablets;
        private final TestTServers testTServers;;
        private final Map<String,String> tableProps;

        public MyBalancerEnvironment(TableId tableId, List<TabletId> tablets, TestTServers testTServers, Map<String,String> tableProps) {
            this.tableId = tableId;
            this.tablets = tablets;
            this.testTServers = testTServers;
            this.tableProps = tableProps;
        }

        @Override
        public Configuration getConfiguration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Configuration getConfiguration(TableId tableId) {
            Configuration mock = EasyMock.mock(ServiceEnvironment.Configuration.class);
            EasyMock.expect(mock.get(EasyMock.anyString())).andAnswer(() -> tableProps.get((String) EasyMock.getCurrentArgument(0))).anyTimes();
            EasyMock.expect(mock.getTableCustom(EasyMock.anyString()))
                            .andAnswer(() -> tableProps.get("table.custom." + (String) EasyMock.getCurrentArgument(0))).anyTimes();
            EasyMock.replay(mock);
            return mock;
        }

        @Override
        public String getTableName(TableId tableId) throws TableNotFoundException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T instantiate(String className, Class<T> base) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T instantiate(TableId tableId, String className, Class<T> base) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String,TableId> getTableIdMap() {
            return Map.of("shard", tableId);
        }

        @Override
        public boolean isTableOnline(TableId tableId) {
            return true;
        }

        @Override
        public Map<TabletId,TabletServerId> listTabletLocations(TableId tableId) {
            Preconditions.checkArgument(this.tableId.equals(tableId));
            Map<TabletId,TabletServerId> allTablets = new HashMap<>();
            tablets.forEach(tabletId -> allTablets.put(tabletId, null));
            allTablets.putAll(testTServers.getLocationProvider());
            return allTablets;
        }

        @Override
        public List<TabletStatistics> listOnlineTabletsForTable(TabletServerId tabletServerId, TableId tableId)
                        throws AccumuloException, AccumuloSecurityException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String tableContext(TableId tableId) {
            throw new UnsupportedOperationException();
        }
    }

}
