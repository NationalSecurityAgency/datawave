package datawave.ingest.table.volumeChoosers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class ShardedTableDateBasedTieredVolumeChooserTest extends EasyMockSupport {
    private static final TableId TABLE_ID = TableId.of("5");
    private static final String NEW_VOLUMES = "newData1,newData2,newData3";
    private static final String OLD_VOLUMES = "oldData1,oldData2";
    private static final Set<String> OPTIONS = Set.of("newData1", "newData2", "newData3", "oldData1", "oldData2");

    @Mock
    private VolumeChooserEnvironment env;
    @Mock
    private ServiceEnvironment serviceEnvironment;
    @Mock
    private ServiceEnvironment.Configuration configuration;

    @Test
    public void testDefaultTabletUsesNewestTier() {
        setupMock(defaultTiers(), null, null);
        assertTrue(Set.of("newData1", "newData2", "newData3").contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testOldDataUsesOldTier() {
        setupMock(defaultTiers(), new Text("20000202_123"), null);
        assertTrue(Set.of("oldData1", "oldData2").contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testDateWithoutShardSuffixUsesOldTier() {
        setupMock(defaultTiers(), new Text("20000202"), "random");
        assertTrue(Set.of("oldData1", "oldData2").contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testFutureDataUsesNewestTier() {
        setupMock(defaultTiers(), new Text("30000202_123"), null);
        assertTrue(Set.of("newData1", "newData2", "newData3").contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testTierBoundaryRemainsInclusive() {
        String boundaryShard = LocalDate.now().minusDays(125).format(DateTimeFormatter.BASIC_ISO_DATE) + "_1";
        setupMock(defaultTiers(), new Text(boundaryShard), "rendezvous");
        assertTrue(Set.of("oldData1", "oldData2").contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testNonTableScopePreservesSuperclassBehavior() {
        resetAll();
        EasyMock.expect(env.getTable()).andReturn(Optional.of(TABLE_ID)).once();
        EasyMock.expect(env.getChooserScope()).andReturn(VolumeChooserEnvironment.Scope.LOGGER).once();
        replayAll();
        assertTrue(OPTIONS.contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testRendezvousIsStableAcrossCallsAndChooserInstances() {
        setupMock(defaultTiers(), new Text("30000202_123"), "  ReNdEzVoUs ");
        String expected = new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS);
        for (int i = 0; i < 100; i++) {
            assertEquals(expected, new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS));
        }
        assertTrue(Set.of("newData1", "newData2", "newData3").contains(expected));
    }

    @Test
    public void testRendezvousIsIndependentOfVolumeIterationOrder() {
        Set<String> forward = new LinkedHashSet<>(Arrays.asList("volumeA", "volumeB", "volumeC"));
        Set<String> reverse = new LinkedHashSet<>(Arrays.asList("volumeC", "volumeB", "volumeA"));
        Text endRow = new Text("20260820_7");
        assertEquals(ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, endRow, forward),
                        ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, endRow, reverse));
    }

    @Test
    public void testNullAndMalformedEndRowsAreStable() {
        Set<String> volumes = Set.of("volumeA", "volumeB");
        assertStable(null, volumes);
        setupMock(defaultTiers(), new Text("not-a-shard"), "rendezvous");
        String choice = new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS);
        assertEquals(choice, new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS));
        assertTrue(OPTIONS.contains(choice));
    }

    @Test
    public void testRawNonUtf8EndRowsAreStable() {
        Text rawEndRow = new Text();
        rawEndRow.set(new byte[] {'a', 0, (byte) 0xff, 'b'});
        assertStable(rawEndRow, Set.of("volumeA", "volumeB"));
    }

    @Test
    public void testConfiguredVolumesAreTrimmed() {
        Map<Long,String> tiers = new LinkedHashMap<>();
        tiers.put(0L, " , newData1, newData2 ,, newData3, ");
        tiers.put(125L, OLD_VOLUMES);
        setupMock(tiers, new Text("30000202_123"), "rendezvous");
        assertTrue(Set.of("newData1", "newData2", "newData3").contains(new ShardedTableDateBasedTieredVolumeChooser().choose(env, OPTIONS)));
    }

    @Test
    public void testMissingConfiguredVolumeFailsClearly() {
        setupMock(defaultTiers(), new Text("30000202_123"), "rendezvous");
        assertConfigurationFailure(Set.of("oldData1", "oldData2"), "table 5", "tier tier0", "newData1", "Accumulo supplied options");
    }

    @Test
    public void testMissingConfiguredVolumeAlsoFailsInRandomMode() {
        setupMock(defaultTiers(), new Text("20000202_123"), null);
        assertConfigurationFailure(Set.of("newData1", "newData2", "newData3"), "table 5", "tier tier125", "oldData1");
    }

    @Test
    public void testEmptyTierFailsClearly() {
        Map<Long,String> tiers = new LinkedHashMap<>();
        tiers.put(0L, " , , ");
        setupMock(tiers, null, "rendezvous");
        assertConfigurationFailure(OPTIONS, "Volumes list empty", "tier tier0", "table 5");
    }

    @Test
    public void testUnknownPlacementFailsClearly() {
        setupMock(defaultTiers(), new Text("30000202_123"), "sticky-ish");
        assertConfigurationFailure(OPTIONS, "sticky-ish", "table.custom.volume.tiered.placement", "random", "rendezvous");
    }

    @Test
    public void testNegativeDaysBackFails() {
        Map<Long,String> tiers = new LinkedHashMap<>();
        tiers.put(-1L, OLD_VOLUMES);
        setupMock(tiers, new Text("20200101_123"), null);
        assertConfigurationFailure(OPTIONS, "Invalid days back", "tier tier-1", "table 5");
    }

    @Test
    public void testRendezvousDistribution() {
        Set<String> volumes = Set.of("volumeA", "volumeB");
        Map<String,Integer> counts = new LinkedHashMap<>();
        counts.put("volumeA", 0);
        counts.put("volumeB", 0);
        for (int i = 0; i < 10_000; i++) {
            String choice = ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, new Text("tablet-" + i), volumes);
            counts.put(choice, counts.get(choice) + 1);
        }
        counts.forEach((volume, count) -> assertTrue(volume + " received " + count + " assignments", count >= 4_000 && count <= 6_000));
    }

    @Test
    public void testAddingAndRemovingVolumeOnlyRemapsToOrFromChangedVolume() {
        Set<String> twoVolumes = Set.of("volumeA", "volumeB");
        Set<String> threeVolumes = Set.of("volumeA", "volumeB", "volumeC");
        for (int i = 0; i < 10_000; i++) {
            Text endRow = new Text("tablet-" + i);
            String withTwo = ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, endRow, twoVolumes);
            String withThree = ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, endRow, threeVolumes);
            if (!"volumeC".equals(withThree)) {
                assertEquals(withTwo, withThree);
            }
        }
    }

    @Test
    public void testRendezvousRejectsEmptyCandidateSet() {
        try {
            ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, null, Collections.emptySet());
            fail("Expected an empty candidate set to fail");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("empty set"));
        }
    }

    private void assertStable(Text endRow, Set<String> volumes) {
        String expected = ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, endRow, volumes);
        for (int i = 0; i < 100; i++) {
            assertEquals(expected, ShardedTableDateBasedTieredVolumeChooser.chooseRendezvous(TABLE_ID, endRow, volumes));
        }
        assertTrue(volumes.contains(expected));
    }

    private void assertConfigurationFailure(Set<String> suppliedOptions, String... messageParts) {
        try {
            new ShardedTableDateBasedTieredVolumeChooser().choose(env, suppliedOptions);
            fail("Expected invalid configuration to fail");
        } catch (IllegalStateException e) {
            for (String messagePart : messageParts) {
                assertTrue("Expected message to contain '" + messagePart + "' but was: " + e.getMessage(), e.getMessage().contains(messagePart));
            }
        }
    }

    private Map<Long,String> defaultTiers() {
        Map<Long,String> tiers = new LinkedHashMap<>();
        tiers.put(0L, NEW_VOLUMES);
        tiers.put(125L, OLD_VOLUMES);
        return tiers;
    }

    private void setupMock(Map<Long,String> tiers, Text endRow, String placement) {
        resetAll();
        EasyMock.expect(env.getTable()).andReturn(Optional.of(TABLE_ID)).anyTimes();
        EasyMock.expect(env.getChooserScope()).andReturn(VolumeChooserEnvironment.Scope.TABLE).anyTimes();
        EasyMock.expect(env.getServiceEnv()).andReturn(serviceEnvironment).anyTimes();
        EasyMock.expect(serviceEnvironment.getConfiguration(TABLE_ID)).andReturn(configuration).anyTimes();
        EasyMock.expect(env.getEndRow()).andReturn(endRow).anyTimes();
        EasyMock.expect(configuration.getTableCustom("volume.tier.names"))
                        .andReturn(tiers.keySet().stream().map(days -> "tier" + days).collect(java.util.stream.Collectors.joining(","))).anyTimes();
        for (Map.Entry<Long,String> tier : tiers.entrySet()) {
            EasyMock.expect(configuration.getTableCustom("volume.tiered.tier" + tier.getKey() + ".volumes")).andReturn(tier.getValue()).anyTimes();
            EasyMock.expect(configuration.getTableCustom("volume.tiered.tier" + tier.getKey() + ".days.back")).andReturn(String.valueOf(tier.getKey()))
                            .anyTimes();
        }
        EasyMock.expect(configuration.getTableCustom("volume.tiered.placement")).andReturn(placement).anyTimes();
        replayAll();
    }
}
