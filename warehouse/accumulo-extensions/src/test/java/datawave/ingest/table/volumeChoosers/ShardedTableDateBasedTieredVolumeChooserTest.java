package datawave.ingest.table.volumeChoosers;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ShardedTableDateBasedTieredVolumeChooserTest {
    Set<String> options = new HashSet<>();
    {
        options.add("newData1");
        options.add("newData2");
        options.add("newData3");
        options.add("oldData1");
        options.add("oldData2");
    }
    @Mock
    VolumeChooserEnvironment env;

    @Mock
    TableId tableId;
    @Mock
    ServiceEnvironment serviceEnvironment;
    @Mock
    ServiceEnvironment.Configuration configuration;

    @Test
    public void testDefaultTablet() {

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        setupMock(tiers, null);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));
    }

    @Test
    public void testAllValidInputOldData() {

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));
    }

    @Test
    public void testAllValidInputNoSuffix() {

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20000202";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));
    }

    @Test
    public void testAllValidInputNewData() {

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));
    }

    @Test
    public void testNewVolumesNotInOptionsForNewData() {
        Set<String> oldOnlyOptions = new HashSet<>();
        oldOnlyOptions.add("oldData1");
        oldOnlyOptions.add("oldData2");

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, oldOnlyOptions);
        assertTrue(newVolumes.contains(choice));

    }

    @Test
    public void testOldVolumesNotInOptionsForOldData() {
        Set<String> newOnlyOptions = new HashSet<>();
        newOnlyOptions.add("newData1");
        newOnlyOptions.add("newData2");
        newOnlyOptions.add("newData3");

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, newOnlyOptions);
        assertTrue(oldVolumes.contains(choice));
    }

    @Test
    public void testShardIdDoesntMatchDatePattern() {
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "HeyThatsNotADate";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(options.contains(choice));

    }

    @Test
    public void testNegativeDaysBack() {
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = -125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20200101_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        assertThrows(IllegalStateException.class, () -> chooser.choose(env, options));

    }

    @Test
    public void testZeroBack() {
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 0;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));

    }

    @Test
    public void testVeryLargeDaysBack() {
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 999999999999L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));

    }

    @Test
    public void testMalformedNewVolumes() {
        String newVolumes = ",newData1,newData2,newData3,,";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 100L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));

    }

    @Test
    public void testMalformedOldVolumes() {
        String newVolumes = ",newData1,newData2,newData3,,";
        String oldVolumes = ",oldData1,,,oldData2,,,";
        long daysBack = 100L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));

    }

    @Test
    public void testEmptyEndRow() {
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 100L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(options.contains(choice));

    }

    @Test
    public void testValidPattern_NewData_optionsDoNotIncludeVolume() {
        Set<String> letterOptions = new HashSet<>();
        letterOptions.add("a");
        letterOptions.add("b");
        letterOptions.add("c");
        letterOptions.add("d");
        letterOptions.add("e");

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;

        Map<Long,String> tiers = new HashMap<>();
        tiers.put(0L, newVolumes);
        tiers.put(daysBack, oldVolumes);

        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(choice.contains("newData"));
    }

    private void setupMock(Map<Long,String> tiers, String shardId) {
        Optional<TableId> tableOptional = Optional.of(tableId);
        lenient().doReturn(VolumeChooserEnvironment.Scope.TABLE).when(env).getChooserScope();
        lenient().doReturn(tableOptional).when(env).getTable();
        lenient().doReturn(serviceEnvironment).when(env).getServiceEnv();
        lenient().doReturn(configuration).when(serviceEnvironment).getConfiguration(tableId);
        doReturn(tiers.keySet().stream().map(s -> "tier" + s).collect(Collectors.joining(","))).when(configuration).getTableCustom("volume.tier.names");
        for (Map.Entry<Long,String> tier : tiers.entrySet()) {
            lenient().doReturn(tier.getValue()).when(configuration).getTableCustom("volume.tiered.tier" + tier.getKey() + ".volumes");
            lenient().doReturn(String.valueOf(tier.getKey())).when(configuration).getTableCustom("volume.tiered.tier" + tier.getKey() + ".days.back");
        }
        if (shardId != null) {
            lenient().doReturn(new Text(shardId), null).when(env).getEndRow();
        }
    }
}
