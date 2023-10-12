package datawave.util.flag.config;

import static datawave.util.flag.FlagMakerTest.CONFIG_BASE_HDFS_DIR;
import static datawave.util.flag.config.FlagMakerConfigUtility.SAMPLE_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.FlagFileTestSetup;

public class FlagMakerConfigUtilityTest {
    private static final Logger LOG = LoggerFactory.getLogger(FlagMakerConfigUtilityTest.class);
    public static final String TEST_CONFIG = "target/test-classes/TestFlagMakerConfig.xml";

    private static final String CONFIG_FLAG_FILE_DIR = "target/test/flags";
    private static final Object CONFIG_EXTRA_INGEST_ARGS = null;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static FlagMakerConfig baseLineFlagMakerConfig;

    @Before
    public void before() {
        try {
            baseLineFlagMakerConfig = FlagMakerConfigUtility.parseArgs(new String[] {"-flagConfig", TEST_CONFIG});
        } catch (Exception e) {
            LOG.error("Failed to create baseline configuration.");
        }
    }

    @Test
    public void parsesBaselineTestConfiguration() {
        // deserialized
        Assert.assertNotNull(baseLineFlagMakerConfig);

        // overridable via command line arguments
        assertEquals(CONFIG_FLAG_FILE_DIR, baseLineFlagMakerConfig.getFlagFileDirectory());
        assertEquals(CONFIG_BASE_HDFS_DIR, baseLineFlagMakerConfig.getBaseHDFSDir());
        assertEquals(datawave.util.flag.FlagMaker.class.getName(), baseLineFlagMakerConfig.getFlagMakerClass());
        assertEquals("target/flagMetrics", baseLineFlagMakerConfig.getFlagMetricsDirectory());
        assertEquals(CONFIG_EXTRA_INGEST_ARGS, baseLineFlagMakerConfig.getDefaultCfg().getExtraIngestArgs());

        // not currently configurable via command line arguments
        assertEquals("file://target", baseLineFlagMakerConfig.getHdfs());
        assertEquals("target/test", baseLineFlagMakerConfig.getDatawaveHome());
        assertEquals(22222, baseLineFlagMakerConfig.getSocketPort());
        // unspecified in the file
        assertEquals(300000, baseLineFlagMakerConfig.getTimeoutMilliSecs());
        // unspecified in the file
        assertEquals(15000, baseLineFlagMakerConfig.getSleepMilliSecs());
        // unspecified in the file
        assertEquals(Integer.MIN_VALUE, baseLineFlagMakerConfig.getFlagCountThreshold());
        // unspecified in the file
        assertEquals(Integer.MAX_VALUE, baseLineFlagMakerConfig.getMaxFileLength());
        assertTrue(baseLineFlagMakerConfig.isSetFlagFileTimestamp());
        assertEquals(1, baseLineFlagMakerConfig.getMaxHdfsThreads());
        assertEquals(2, baseLineFlagMakerConfig.getDirectoryCacheSize());
        assertEquals(5000, baseLineFlagMakerConfig.getDirectoryCacheTimeout());
        assertEquals(datawave.util.flag.processor.SimpleFlagDistributor.class.getName(), baseLineFlagMakerConfig.getFlagDistributorClass());
        assertEquals(2, baseLineFlagMakerConfig.getFilePatterns().size());
        assertTrue(baseLineFlagMakerConfig.getFilePatterns().contains("2*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]"));
        assertTrue(baseLineFlagMakerConfig.getFilePatterns().contains("2*/*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]"));
    }

    @Test
    public void parsesDataTypeConfigsWithinBaselineConfig() {
        FlagDataTypeConfig defaultCfg = baseLineFlagMakerConfig.getDefaultCfg();
        assertNull(defaultCfg.getDataName());
        assertEquals(10, defaultCfg.getMaxFlags());
        assertEquals(10, defaultCfg.getReducers());
        assertEquals("bin/ingest/bulk-ingest.sh", defaultCfg.getScript());

        List<FlagDataTypeConfig> flagConfigs = baseLineFlagMakerConfig.getFlagConfigs();
        assertEquals(1, flagConfigs.size());

        FlagDataTypeConfig flagDataTypeConfig = flagConfigs.get(0);
        assertEquals("foo", flagDataTypeConfig.getDataName());
        assertEquals("none", flagDataTypeConfig.getDistributionArgs());
        assertEquals("foo,bar", flagDataTypeConfig.getFolder());
        assertEquals(datawave.ingest.input.reader.event.EventSequenceFileInputFormat.class, flagDataTypeConfig.getInputFormat());
        assertEquals("onehr", flagDataTypeConfig.getIngestPool());
        assertFalse(flagDataTypeConfig.isLifo());
        assertEquals(10, flagDataTypeConfig.getMaxFlags());
        assertEquals(10, flagDataTypeConfig.getReducers());
        assertEquals("bin/ingest/bulk-ingest.sh", flagDataTypeConfig.getScript());
    }

    @Test
    public void withFlagFileDirectoryOverride() throws Exception {
        final String overrideValue = "/srv/data/somewhere/else/";

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility
                        .parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-flagFileDirectoryOverride", overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getFlagFileDirectory());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.setFlagFileDirectory(overrideValue);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void withHdfsOverride() throws Exception {
        String overrideValue = "testDir/BulkIngest/";

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-baseHDFSDirOverride", overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getBaseHDFSDir());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.setBaseHDFSDir(overrideValue);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void withFlagMakerClassOverride() throws Exception {
        String overrideValue = "SomeClass";

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-flagMakerClass", overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getFlagMakerClass());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.setFlagMakerClass(overrideValue);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void withMetricsDirectoryOverride() throws Exception {
        String overrideValue = "testDir/OtherMetricsDirectory";

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-flagMetricsDirectory", overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getFlagMetricsDirectory());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.setFlagMetricsDirectory(overrideValue);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void withSleepMilliSecs() throws Exception {
        long overrideValue = 987654321L;

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-sleepMilliSecs", "" + overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getSleepMilliSecs());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.setSleepMilliSecs(overrideValue);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void withSocket() throws Exception {
        int overrideValue = 12345;

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-socket", "" + overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getSocketPort());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.setSocketPort(overrideValue);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void withExtraArgsOverride() throws Exception {
        final String overrideValue = "-fastMode -topSpeed=MAX";

        // create a new FlagMakerConfig with one override
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility
                        .parseArgs(new String[] {"-flagConfig", TEST_CONFIG, "-extraIngestArgsOverride", overrideValue});

        // verify override
        assertEquals(overrideValue, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());

        // verify FlagMakerConfig matches baseline except for the single override
        baseLineFlagMakerConfig.getDefaultCfg().setExtraIngestArgs(overrideValue);
        assertEquals(overrideValue, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());
        baseLineFlagMakerConfig.getFlagConfigs().get(0).setExtraIngestArgs(overrideValue);
        assertEquals(overrideValue, flagMakerConfig.getFlagConfigs().get(0).getExtraIngestArgs());
    }

    @Test
    public void withAllOverrides() throws Exception {
        // create a new FlagMakerConfig with every available override
        String flagFileDirectoryOverride = "/srv/data/somewhere/else/";
        String baseDirOverride = "testDir/BulkIngest/";
        String overrideClass = "SomeClass";
        String overrideMetricsDirectory = "testDir/OtherMetricsDirectory";
        String overrideExtraArgs = "-fastMode -topSpeed=MAX";
        String overrideSocket = "12345";
        String overrideSleepMillis = "987654321";

        // @formatter:off
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility
                .parseArgs(new String[]{
                        "-flagConfig", TEST_CONFIG,
                        "-flagFileDirectoryOverride", flagFileDirectoryOverride,
                        "-baseHDFSDirOverride", "testDir/BulkIngest/",
                        "-socket", overrideSocket,
                        "-sleepMilliSecs", overrideSleepMillis,
                        "-flagMakerClass", overrideClass,
                        "-flagMetricsDirectory", overrideMetricsDirectory,
                        "-extraIngestArgsOverride", overrideExtraArgs});
        // @formatter:on

        // verify overrides
        assertEquals(flagFileDirectoryOverride, flagMakerConfig.getFlagFileDirectory());
        assertEquals(baseDirOverride, flagMakerConfig.getBaseHDFSDir());
        assertEquals(overrideClass, flagMakerConfig.getFlagMakerClass());
        assertEquals(Integer.parseInt(overrideSocket), flagMakerConfig.getSocketPort());
        assertEquals(Long.parseLong(overrideSleepMillis), flagMakerConfig.getSleepMilliSecs());
        assertEquals(overrideMetricsDirectory, flagMakerConfig.getFlagMetricsDirectory());
        assertEquals(overrideExtraArgs, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());

        // verify FlagMakerConfig matches baseline except for the overrides
        baseLineFlagMakerConfig.setFlagFileDirectory(flagFileDirectoryOverride);
        baseLineFlagMakerConfig.setBaseHDFSDir(baseDirOverride);
        baseLineFlagMakerConfig.setFlagMakerClass(overrideClass);
        baseLineFlagMakerConfig.setSocketPort(Integer.parseInt(overrideSocket));
        baseLineFlagMakerConfig.setSleepMilliSecs(Long.parseLong(overrideSleepMillis));
        baseLineFlagMakerConfig.setFlagMetricsDirectory(overrideMetricsDirectory);
        baseLineFlagMakerConfig.getDefaultCfg().setExtraIngestArgs(overrideExtraArgs);
        baseLineFlagMakerConfig.getFlagConfigs().get(0).setExtraIngestArgs(overrideExtraArgs);
        assertEquals(baseLineFlagMakerConfig, flagMakerConfig);
    }

    @Test
    public void deserializeAndSerializeFilesIdentical() throws Exception {
        // generate sample file
        new File(SAMPLE_FILE_NAME).deleteOnExit();
        FlagMakerConfigUtility.createSample();
        File generatedFile = new File(SAMPLE_FILE_NAME);

        // deserialize
        FlagMakerConfig xmlObject = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(SAMPLE_FILE_NAME));

        // serialize to a new file name
        File reserializedFile = folder.newFile("reserializesIdenticalSample.xml");
        FlagMakerConfigUtility.saveXmlObject(xmlObject, reserializedFile);

        // compare files
        Assert.assertArrayEquals(Files.readAllBytes(generatedFile.toPath()), Files.readAllBytes(reserializedFile.toPath()));
    }

    @Test
    public void serializeAndDeserializeObjectsIdentical() throws Exception {
        // deserialize test configuration file
        FlagMakerConfig originalConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));

        // serialize to a new file
        File reserializedFile = folder.newFile("serializedBackToFile.xml");
        FlagMakerConfigUtility.saveXmlObject(originalConfig, reserializedFile);

        // deserialized reserialized file
        FlagMakerConfig deserializedAgain = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(reserializedFile.getPath()));

        // compare objects
        assertEquals(originalConfig, deserializedAgain);
    }

    @Test
    public void canCreateDirectoryWithGetHadoopFs() throws Exception {
        // Create test subject
        FlagMakerConfig flagMakerConfig = new FlagFileTestSetup().withTestNameForDirectories("FlagMakerTest_canCreateDirectoryWithGetHadoopFs")
                        .withTestFlagMakerConfig().getFlagMakerConfig();
        FileSystem fileSystem = FlagMakerConfigUtility.getHadoopFS(flagMakerConfig);

        // ensure directory is eliminate if it already exists
        File flagDirectory = new File(flagMakerConfig.getFlagFileDirectory());
        java.nio.file.Files.delete(flagDirectory.toPath());
        assertTrue("Could not delete " + flagDirectory.toString(), !flagDirectory.exists());

        // verify the FileSystem is functional
        fileSystem.mkdirs(new org.apache.hadoop.fs.Path(flagDirectory.toString()));
        assertTrue("Unable to create directory from getHadoopFS utility using config", flagDirectory.exists());
        assertTrue("Unable to create directory from getHadoopFS utility using config", flagDirectory.isDirectory());
    }
}
