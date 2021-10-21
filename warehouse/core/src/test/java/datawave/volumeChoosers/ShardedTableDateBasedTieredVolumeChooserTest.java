package datawave.volumeChoosers;
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
import java.util.stream.Collectors;

import static org.junit.Assert.*;


@RunWith(EasyMockRunner.class)
public class ShardedTableDateBasedTieredVolumeChooserTest extends EasyMockSupport{

    @Mock
    VolumeChooserEnvironment env;

    @Mock
    TableId tableId;
    @Mock
    ServiceEnvironment serviceEnvironment;
    @Mock
    ServiceEnvironment.Configuration configuration;

    @Test
    public void testAllValidInputOldData()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "125";

        String shardId = "20000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(oldVolumes.contains(choice));
    }

    @Test
    public void testAllValidInputNewData()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "125";

        String shardId = "30000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(newVolumes.contains(choice));
    }

    @Test(expected = VolumeChooser.VolumeChooserException.class)
    public void testNewVolumesNotInOptionsForNewData()
    {
        String[] options = new String[2];
        options[0] = "oldData1";
        options[1] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "125";

        String shardId = "30000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        chooser.choose(env,options);
    }

    @Test(expected = VolumeChooser.VolumeChooserException.class)
    public void testOldVolumesNotInOptionsForOldData()
    {
        String[] options = new String[3];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "125";

        String shardId = "20000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        chooser.choose(env,options);
    }

    @Test
    public void testShardIdDoesntMatchDatePattern()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "125";

        String shardId = "HeyThatsNotADate";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(Arrays.asList(options).contains(choice));

    }


    @Test(expected = VolumeChooser.VolumeChooserException.class)
    public void testNegativeDaysBack()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "-125";

        String shardId = "HeyThatsNotADate";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(Arrays.asList(options).contains(choice));

    }

    @Test
    public void testZeroBack()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "0";

        String shardId = "20000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(oldVolumes.contains(choice));

    }


    @Test(expected = VolumeChooser.VolumeChooserException.class)
    public void testVeryLargeDaysBack()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "999999999999";

        String shardId = "20000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(oldVolumes.contains(choice));

    }

    @Test
    public void testMalformedNewVolumes()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = ",newData1,newData2,newData3,,";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "100";

        String shardId = "30000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(newVolumes.contains(choice));

    }

    @Test
    public void testMalformedOldVolumes()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = ",oldData1,,,oldData2,,,";
        String daysBack = "100";

        String shardId = "20000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(oldVolumes.contains(choice));

    }

    @Test
    public void testNullEndRow()
    {
        String[] options = new String[5];
        options[0] = "newData1";
        options[1] = "newData2";
        options[2] = "newData3";
        options[3] = "oldData1";
        options[4] = "oldData2";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = ",oldData1,,,oldData2,,,";
        String daysBack = "100";

        String shardId = "";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertTrue(Arrays.asList(options).contains(choice));

    }


    @Test
    public void testValidPattern_NewData_optionsDoNotIncludeVolume()
    {
        String[] options = new String[5];
        options[0] = "a";
        options[1] = "b";
        options[2] = "c";
        options[3] = "d";
        options[4] = "e";

        String newVolumes = "newData1,newData2,newData3";
        String oldVolumes = "oldData1,oldData2";
        String daysBack = "125";

        String shardId = "30000202_123";
        setupMock(newVolumes, oldVolumes, daysBack, shardId);
        ShardedTableDateBasedTieredVolumeChooser chooser = new ShardedTableDateBasedTieredVolumeChooser();
        String choice = chooser.choose(env,options);
        assertNull(choice);
    }

    private void setupMock(String newVolumes, String oldVolumes, String daysBack, String shardId) {
        resetAll();
        EasyMock.expect(env.hasTableId()).andReturn(true).once();
        EasyMock.expect(env.getScope()).andReturn(VolumeChooserEnvironment.ChooserScope.TABLE).times(2);
        EasyMock.expect(env.getTableId()).andReturn(tableId).times(6);
        EasyMock.expect(env.getServiceEnv()).andReturn(serviceEnvironment).times(4);
        EasyMock.expect(serviceEnvironment.getConfiguration(tableId)).andReturn(configuration).anyTimes();
        EasyMock.expect(configuration.getTableCustom("volume.dates.tiered.new")).andReturn(newVolumes).once();
        EasyMock.expect(configuration.getTableCustom("volume.dates.tiered.old")).andReturn(oldVolumes).once();
        EasyMock.expect(configuration.getTableCustom("volume.dates.tiered.days.back")).andReturn(daysBack).once();
        EasyMock.expect(env.getEndRow()).andReturn(new Text(shardId)).once();
        replayAll();
    }
}
