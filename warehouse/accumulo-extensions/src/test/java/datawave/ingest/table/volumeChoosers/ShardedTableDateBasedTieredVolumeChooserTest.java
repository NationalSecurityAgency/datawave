package datawave.ingest.table.volumeChoosers;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.server.fs.VolumeChooser;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@RunWith(EasyMockRunner.class)
public class ShardedTableDateBasedTieredVolumeChooserTest extends EasyMockSupport {
    
    @Mock
    VolumeChooserEnvironment env;
    
    @Mock
    TableId tableId;
    @Mock
    ServiceEnvironment serviceEnvironment;
    @Mock
    ServiceEnvironment.Configuration configuration;
    
    @Test
    public void testAllValidInputOldData() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));
    }
    
    @Test
    public void testAllValidInputNewData() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));
    }
    
    @Test
    public void testNewVolumesNotInOptionsForNewData() {
        String[] options = new String[2];
        options[0] = "oldData1";
        options[1] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(choice.contains("newData"));
        
    }
    
    @Test
    public void testOldVolumesNotInOptionsForOldData() {
        String[] options = new String[3];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        chooser.choose(env, options);
    }
    
    @Test
    public void testShardIdDoesntMatchDatePattern() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "HeyThatsNotADate";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(Arrays.asList(options).contains(choice));
        
    }
    
    @Test(expected = VolumeChooser.VolumeChooserException.class)
    public void testNegativeDaysBack() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = -125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "20200101_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(Arrays.asList(options).contains(choice));
        
    }
    
    @Test
    public void testZeroBack() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 0;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));
        
    }
    
    @Test
    public void testVeryLargeDaysBack() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 999999999999L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));
        
    }
    
    @Test
    public void testMalformedNewVolumes() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = ",newData1,newData2,newData3,,";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 100L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(newVolumes.contains(choice));
        
    }
    
    @Test
    public void testMalformedOldVolumes() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = ",newData1,newData2,newData3,,";
        String oldVolumes = ",oldData1,,,oldData2,,,";
        long daysBack = 100L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "20000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(oldVolumes.contains(choice));
        
    }
    
    @Test
    public void testEmptyEndRow() {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = ",oldData1,,,oldData2,,,";
        long daysBack = 100L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "";
        // setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
        assertTrue(Arrays.asList(options).contains(choice));
        
    }
    
    @Test
    public void testValidPattern_NewData_optionsDoNotIncludeVolume() {
        String[] options = new String[5];
        options[0] = "a";
        options[1] = "b";
        options[2] = "c";
        options[3] = "d";
        options[4] = "e";
        
        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        long daysBack = 125L;
        
        Map<Long,String> tiers = new HashMap<>();
        tiers.put(daysBack, newVolumes);
        tiers.put(Long.MAX_VALUE, oldVolumes);
        
        String shardId = "30000202_123";
        setupMock(tiers, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env, options);
    }
    
    private void setupMock(Map<Long,String> tiers, String shardId) {
        resetAll();
        EasyMock.expect(env.hasTableId()).andReturn(true).once();
        EasyMock.expect(env.getScope()).andReturn(VolumeChooserEnvironment.ChooserScope.TABLE).times(1);
        EasyMock.expect(env.getTableId()).andReturn(tableId).anyTimes();
        EasyMock.expect(env.getServiceEnv()).andReturn(serviceEnvironment).anyTimes();
        EasyMock.expect(serviceEnvironment.getConfiguration(tableId)).andReturn(configuration).anyTimes();
        EasyMock.expect(configuration.getTableCustom("volume.tier.names"))
                        .andReturn(tiers.keySet().stream().map(s -> "tier" + s).collect(Collectors.joining(","))).once();
        for (Map.Entry<Long,String> tier : tiers.entrySet()) {
            EasyMock.expect(configuration.getTableCustom("volume.tiered.tier" + tier.getKey() + ".volumes")).andReturn(tier.getValue()).once();
            EasyMock.expect(configuration.getTableCustom("volume.tiered.tier" + tier.getKey() + ".days.back")).andReturn(String.valueOf(tier.getKey()))
                            .anyTimes();
        }
        EasyMock.expect(env.getEndRow()).andReturn(new Text(shardId)).once();
        replayAll();
    }
}
