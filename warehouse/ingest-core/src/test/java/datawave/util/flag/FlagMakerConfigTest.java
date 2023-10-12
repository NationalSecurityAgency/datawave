package datawave.util.flag;

import static datawave.util.flag.config.FlagMakerConfigUtilityTest.TEST_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.Collection;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;
import datawave.util.flag.processor.FlagDistributor;
import datawave.util.flag.processor.SizeValidator;

public class FlagMakerConfigTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void defaultDataTypeConfigIsNull() {
        FlagMakerConfig flagMakerConfig = new FlagMakerConfig();
        assertNull(flagMakerConfig.getDefaultCfg());
    }

    @Test
    public void addsDataTypeConfig() {
        new FlagMakerConfig();
    }

    @Test
    public void testDefaultsToString() {
        assertNotNull(new FlagMakerConfig().toString());
    }

    @Test
    public void testFileToString() throws Exception {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        String result = flagMakerConfig.toString();
        assertNotNull(result);
        assertTrue(result, result.contains("hdfs: file://target"));
        assertTrue(result, result.contains("datawaveHome: target/test"));
        assertTrue(result, result.contains("baseHDFSDir: target/test/BulkIngest"));
        assertTrue(result, result.contains("socketPort: 22222"));
        assertTrue(result, result.contains("flagFileDirectory: target/test/flags"));
        assertTrue(result, result.contains("filePatterns: [2*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z], 2*/*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]]"));
        assertTrue(result, result.contains("timeoutMilliSecs: 300000"));
        assertTrue(result, result.contains("sleepMilliSecs: 15000"));
        assertTrue(result, result.contains("flagCountThreshold: -2147483648"));
        assertTrue(result, result.contains("maxFileLength: 2147483647"));
        assertTrue(result, result.contains("isSetFlagFileTimestamp: true"));
        assertTrue(result, result.contains("useFolderTimestamp: false"));
        assertTrue(result, result.contains("flagMetricsDirectory: target/flagMetrics"));
        assertTrue(result, result.contains("maxHdfsThreads: 1"));
        assertTrue(result, result.contains("directoryCacheSize: 2"));
        assertTrue(result, result.contains("directoryCacheTimeout: 5000"));
        assertTrue(result, result.contains("flagMakerClass: datawave.util.flag.FlagMaker"));
        assertTrue(result, result.contains(
                        "defaultCfg: FlagDataTypeConfig{dataName='null', folders=null, inputFormat=class datawave.ingest.input.reader.event.EventSequenceFileInputFormat, reducers=10, maxFlags=10, ingestPool='null', extraIngestArgs='null', distributionArgs='none', lifo=false, timeoutMilliSecs=-2147483648, flagCountThreshold=-2147483648, script='bin/ingest/bulk-ingest.sh', fileListMarker='null', collectMetrics='null'"));
        // note: previously this asserted folders=[foo,bar] which was a list
        // containing a single String of value "foo,bar"
        // now it asserts that the value is [foo, bar] which is a list
        // containing a String "foo" and another String "bar"
        assertTrue(result, result.contains(
                        "flagCfg: [FlagDataTypeConfig{dataName='foo', folders=[foo, bar], inputFormat=class datawave.ingest.input.reader.event.EventSequenceFileInputFormat, reducers=0, maxFlags=0, ingestPool='onehr', extraIngestArgs='null', distributionArgs='none', lifo=false, timeoutMilliSecs=-2147483648, flagCountThreshold=-2147483648, script='null', fileListMarker='null', collectMetrics='null'"));
    }

    @Test
    public void afterDeserializeAndReserialize() throws Exception {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));

        File reserializedFile = folder.newFile("sameResultAfterDeserializeAndReserialize.xml");
        FlagMakerConfigUtility.saveXmlObject(flagMakerConfig, reserializedFile);
        byte[] actualBytes = Files.readAllBytes(reserializedFile.toPath());

        String expectedContents = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + "<flagMakerConfig>\n" + "    <flagCfg>\n"
                        + "        <dataName>foo</dataName>\n" + "        <distributionArgs>none</distributionArgs>\n"
                        + "        <flagCountThreshold>-2147483648</flagCountThreshold>\n" + "        <folder>foo,bar</folder>\n"
                        + "        <ingestPool>onehr</ingestPool>\n"
                        + "        <inputFormat>datawave.ingest.input.reader.event.EventSequenceFileInputFormat</inputFormat>\n"
                        + "        <lifo>false</lifo>\n" + "        <maxFlags>0</maxFlags>\n" + "        <reducers>0</reducers>\n"
                        + "        <timeoutMilliSecs>-2147483648</timeoutMilliSecs>\n" + "    </flagCfg>\n"
                        + "    <filePattern>2*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]</filePattern>\n"
                        + "    <filePattern>2*/*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]</filePattern>\n" + "    <baseHDFSDir>target/test/BulkIngest/</baseHDFSDir>\n" + // validate
                                                                                                                                                             // adds
                                                                                                                                                             // trailing
                                                                                                                                                             // slash
                        "    <datawaveHome>target/test</datawaveHome>\n" + "    <defaultCfg>\n" + "        <distributionArgs>none</distributionArgs>\n"
                        + "        <flagCountThreshold>-2147483648</flagCountThreshold>\n"
                        + "        <inputFormat>datawave.ingest.input.reader.event.EventSequenceFileInputFormat</inputFormat>\n"
                        + "        <lifo>false</lifo>\n" + "        <maxFlags>10</maxFlags>\n" + "        <reducers>10</reducers>\n"
                        + "        <script>bin/ingest/bulk-ingest.sh</script>\n" + "        <timeoutMilliSecs>-2147483648</timeoutMilliSecs>\n"
                        + "    </defaultCfg>\n" + "    <directoryCacheSize>2</directoryCacheSize>\n"
                        + "    <directoryCacheTimeout>5000</directoryCacheTimeout>\n" + "    <flagCountThreshold>-2147483648</flagCountThreshold>\n"
                        + "    <flagDistributorClass>datawave.util.flag.processor.SimpleFlagDistributor</flagDistributorClass>\n"
                        + "    <flagFileDirectory>target/test/flags</flagFileDirectory>\n"
                        + "    <flagMakerClass>datawave.util.flag.FlagMaker</flagMakerClass>\n"
                        + "    <flagMetricsDirectory>target/flagMetrics</flagMetricsDirectory>\n" + "    <hdfs>file://target</hdfs>\n"
                        + "    <maxFileLength>2147483647</maxFileLength>\n" + "    <maxHdfsThreads>1</maxHdfsThreads>\n"
                        + "    <setFlagFileTimestamp>true</setFlagFileTimestamp>\n" + "    <sleepMilliSecs>15000</sleepMilliSecs>\n"
                        + "    <socketPort>22222</socketPort>\n" + "    <timeoutMilliSecs>300000</timeoutMilliSecs>\n"
                        + "    <useFolderTimestamp>false</useFolderTimestamp>\n" + "</flagMakerConfig>\n";
        Assert.assertArrayEquals(new String(actualBytes), expectedContents.getBytes(), actualBytes);
    }

    @Test
    public void serializeDefaultFlagDataTypeConfig() throws JAXBException {
        FlagMakerConfig flagMakerConfig = new FlagMakerConfig();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        FlagMakerConfigUtility.saveXmlObject(flagMakerConfig, outputStream);
        String serializedFlagMakerConfig = outputStream.toString();
        assertFalse("Unexpected contents: " + serializedFlagMakerConfig, serializedFlagMakerConfig.contains("<last>"));

        // @formatter:off
		String expectedContent = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
				+ "<flagMakerConfig>\n"
				+ "    <baseHDFSDir>/data/ShardIngest</baseHDFSDir>\n"
				+ "    <directoryCacheSize>2000</directoryCacheSize>\n"
				+ "    <directoryCacheTimeout>7200000</directoryCacheTimeout>\n"
				+ "    <flagCountThreshold>-2147483648</flagCountThreshold>\n"
				+ "    <flagDistributorClass>datawave.util.flag.processor.SimpleFlagDistributor</flagDistributorClass>\n"
				+ "    <flagMakerClass>datawave.util.flag.FlagMaker</flagMakerClass>\n"
				+ "    <flagMetricsDirectory>/data/BulkIngest/FlagMakerMetrics</flagMetricsDirectory>\n"
				+ "    <hdfs>hdfs://localhost:9000</hdfs>\n"
				+ "    <maxFileLength>2147483647</maxFileLength>\n"
				+ "    <maxHdfsThreads>25</maxHdfsThreads>\n"
				+ "    <setFlagFileTimestamp>true</setFlagFileTimestamp>\n"
				+ "    <sleepMilliSecs>15000</sleepMilliSecs>\n"
				+ "    <socketPort>0</socketPort>\n"
				+ "    <timeoutMilliSecs>300000</timeoutMilliSecs>\n"
				+ "    <useFolderTimestamp>false</useFolderTimestamp>\n"
				+ "</flagMakerConfig>\n";
		// @formatter:on
        assertEquals("Unexpected contents: " + serializedFlagMakerConfig, expectedContent, serializedFlagMakerConfig);
    }

    @Test
    public void deserializedConfigMatchesDefaultConstructor() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new StringReader("<emptyCfg/>"));
        assertEquals(new FlagMakerConfig(), flagMakerConfig);
    }

    @Test
    public void nowIgnoresDistributorType() throws IOException {
        String flagMakerConfigStr = "<flagMakerConfig>\n" + "<distributorType>simple</distributorType>\n" + "</flagMakerConfig>";
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new StringReader(flagMakerConfigStr));
        assertEquals(new FlagMakerConfig(), flagMakerConfig);
    }

    @Test
    public void supportsDifferentDistributorClassNames() throws IOException {
        String flagMakerConfigStr = "<flagMakerConfig>\n" + "<flagDistributorClass>" + FlagMakerConfigTest.NoOpFlagDistributor.class.getName()
                        + "</flagDistributorClass>\n" + "</flagMakerConfig>";
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new StringReader(flagMakerConfigStr));
        FlagMakerConfig expectedConfig = new FlagMakerConfig();
        expectedConfig.setFlagDistributorClass(FlagMakerConfigTest.NoOpFlagDistributor.class.getName());
        assertEquals(expectedConfig, flagMakerConfig);
        assertNotEquals(new FlagMakerConfig(), flagMakerConfig);
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void validateRejectsInvalidMarkerText() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: fileListMarker cannot contain spaces");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        FlagDataTypeConfig cfg = flagMakerConfig.getFlagConfigs().get(0);
        cfg.setFileListMarker("xxxx  invalid-marker  xxxx");
        flagMakerConfig.validate();
    }

    @Test
    public void validateAcceptsValidMarkerText() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        FlagDataTypeConfig cfg = flagMakerConfig.getFlagConfigs().get(0);
        cfg.setFileListMarker("valid-marker");
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsNullScript() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: default script is required");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        FlagDataTypeConfig cfg = flagMakerConfig.getDefaultCfg();
        cfg.setScript(null);
        flagMakerConfig.validate();
    }

    @Test
    public void validateDefaultScriptRequiredEvenWhenDefinedForEachType() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: default script is required");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        for (FlagDataTypeConfig dataTypeConfig : flagMakerConfig.getFlagConfigs()) {
            dataTypeConfig.setScript("set");
        }
        flagMakerConfig.getDefaultCfg().setScript(null);
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsNullHdfsDirectory() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: baseHDFSDir is required");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setBaseHDFSDir(null);
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsSocketBelowRange() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: socketPort is required and must be greater than 1024 and less than 65535");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setSocketPort(1024);
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsSocketAboveRange() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: socketPort is required and must be greater than 1024 and less than 65535");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setSocketPort(65535);
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsMaxFlagsBelowRange() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: Default Max Flags must be set.");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getDefaultCfg().setMaxFlags(0);
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsMaxFlagsBelowRangeEvenWhenDefinedForEachType() throws IOException {
        exceptionRule.expectMessage("FlagMakerConfig Error: Default Max Flags must be set.");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getDefaultCfg().setMaxFlags(0);

        for (FlagDataTypeConfig dataTypeConfig : flagMakerConfig.getFlagConfigs()) {
            dataTypeConfig.setMaxFlags(10);
        }
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsNullInputFormat() throws IOException {
        exceptionRule.expectMessage("Input Format Class must be specified for data type: ");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setInputFormat(null);
        flagMakerConfig.validate();
    }

    @Test
    public void validateRejectsNullIngestPool() throws IOException {
        exceptionRule.expectMessage("Ingest Pool must be specified for data type: ");
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setIngestPool(null);
        flagMakerConfig.validate();
    }

    @Test
    public void validatePushesBaseHdfsDirIntoDataTypeConfig() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setBaseHDFSDir("override");
        flagMakerConfig.getFlagConfigs().get(0).setBaseHdfsDir(null);
        flagMakerConfig.validate();
        assertEquals("override/", flagMakerConfig.getFlagConfigs().get(0).getBaseHdfsDir());
    }

    @Test
    public void validatePushesBaseHdfsDirIntoDataTypeConfigEvenWhenSet() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setBaseHDFSDir("override");
        flagMakerConfig.getFlagConfigs().get(0).setBaseHdfsDir("value");
        flagMakerConfig.validate();
        assertEquals("override/", flagMakerConfig.getFlagConfigs().get(0).getBaseHdfsDir());
    }

    @Test
    public void validatePushesFlagCountThresholdIntoDataTypeConfig() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setFlagCountThreshold(1);
        flagMakerConfig.getFlagConfigs().get(0).setFlagCountThreshold(Integer.MIN_VALUE);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getFlagCountThreshold());
    }

    @Test
    public void validatePushesTimeoutIntoDataTypeConfig() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.setTimeoutMilliSecs(1);
        flagMakerConfig.getFlagConfigs().get(0).setTimeoutMilliSecs(Integer.MIN_VALUE);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getTimeoutMilliSecs());
    }

    @Test
    public void validateSetsLastForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));

        // initially less than current time
        assertTrue(System.currentTimeMillis() >= flagMakerConfig.getFlagConfigs().get(0).getLast());

        // large timeout at FlagMakerConfig level
        flagMakerConfig.setTimeoutMilliSecs(1000000);
        // unset timeout for DataType
        flagMakerConfig.getFlagConfigs().get(0).setTimeoutMilliSecs(Integer.MIN_VALUE);
        // pushes FlagMakerConfig's timeout down to datatype and sets last
        flagMakerConfig.validate();

        // now greater than current time
        assertTrue(System.currentTimeMillis() < flagMakerConfig.getFlagConfigs().get(0).getLast());
    }

    @Test
    public void validateSetsLastForDataTypeWhenFlagMakerConfigUnset() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));

        // initially less than current time
        assertTrue(System.currentTimeMillis() >= flagMakerConfig.getFlagConfigs().get(0).getLast());

        // set timeout for DataType alone
        flagMakerConfig.getFlagConfigs().get(0).setTimeoutMilliSecs(1000000);

        // pushes FlagMakerConfig's timeout down to datatype and sets last
        flagMakerConfig.validate();

        // now greater than current time
        assertTrue(System.currentTimeMillis() < flagMakerConfig.getFlagConfigs().get(0).getLast());
    }

    @Test
    public void validateSetsLastWhenBothTimeoutsUnset() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));

        // initially less than current time
        assertTrue(System.currentTimeMillis() >= flagMakerConfig.getFlagConfigs().get(0).getLast());

        // both FlagMakerConfig and datatype's timeouts are unset
        flagMakerConfig.getFlagConfigs().get(0).setTimeoutMilliSecs(Integer.MIN_VALUE);
        flagMakerConfig.setTimeoutMilliSecs(Integer.MIN_VALUE);

        flagMakerConfig.validate();

        // still less than current time
        assertTrue(System.currentTimeMillis() >= flagMakerConfig.getFlagConfigs().get(0).getLast());
    }

    @Test
    public void validateDefaultMaxFlagsUsedWhenUnsetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setMaxFlags(0);
        flagMakerConfig.getDefaultCfg().setMaxFlags(1);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getMaxFlags());
    }

    @Test
    public void validateDefaultMaxFlagsIgnoredWhenSetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setMaxFlags(1);
        flagMakerConfig.getDefaultCfg().setMaxFlags(2);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getMaxFlags());
    }

    @Test
    public void validateDefaultReducersUsedWhenUnsetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setReducers(0);
        flagMakerConfig.getDefaultCfg().setReducers(1);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getReducers());
    }

    @Test
    public void validateDefaultReducersUnsetAndDataTypeSet() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setReducers(1);
        flagMakerConfig.getDefaultCfg().setReducers(0);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getReducers());
    }

    @Test
    public void validateAllReducersUnset() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setReducers(0);
        flagMakerConfig.getDefaultCfg().setReducers(0);
        flagMakerConfig.validate();
        assertEquals(0, flagMakerConfig.getFlagConfigs().get(0).getReducers());
    }

    @Test
    public void validateDefaultReducersIgnoredWhenSetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setReducers(1);
        flagMakerConfig.getDefaultCfg().setReducers(2);
        flagMakerConfig.validate();
        assertEquals(1, flagMakerConfig.getFlagConfigs().get(0).getReducers());
    }

    @Test
    public void validateDefaultScriptUsedWhenUnsetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setScript(null);
        flagMakerConfig.getDefaultCfg().setScript("default");
        flagMakerConfig.validate();
        assertEquals("default", flagMakerConfig.getFlagConfigs().get(0).getScript());
    }

    @Test
    public void validateDefaultScriptIgnoredWhenSetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setScript("set");
        flagMakerConfig.getDefaultCfg().setScript("default");
        flagMakerConfig.validate();
        assertEquals("set", flagMakerConfig.getFlagConfigs().get(0).getScript());
    }

    @Test
    public void validateDefaultCollectMetricsUsedWhenUnsetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setCollectMetrics(null);
        flagMakerConfig.getDefaultCfg().setCollectMetrics("default");
        flagMakerConfig.validate();
        assertEquals("default", flagMakerConfig.getFlagConfigs().get(0).getCollectMetrics());
    }

    @Test
    public void validateDefaultCollectMetricsUnsetAndDataTypeSet() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setCollectMetrics("set");
        flagMakerConfig.getDefaultCfg().setCollectMetrics(null);
        flagMakerConfig.validate();
        assertEquals("set", flagMakerConfig.getFlagConfigs().get(0).getCollectMetrics());
    }

    @Test
    public void validateAllCollectMetricsUnset() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setCollectMetrics(null);
        flagMakerConfig.getDefaultCfg().setCollectMetrics(null);
        flagMakerConfig.validate();
        assertNull(flagMakerConfig.getFlagConfigs().get(0).getCollectMetrics());
    }

    @Test
    public void validateDefaultCollectMetricsIgnoredWhenSetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setCollectMetrics("set");
        flagMakerConfig.getDefaultCfg().setCollectMetrics("default");
        flagMakerConfig.validate();
        assertEquals("set", flagMakerConfig.getFlagConfigs().get(0).getCollectMetrics());
    }

    @Test
    public void validateDefaultFileListMarkerUsedWhenUnsetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setFileListMarker(null);
        flagMakerConfig.getDefaultCfg().setFileListMarker("default");
        flagMakerConfig.validate();
        assertEquals("default", flagMakerConfig.getFlagConfigs().get(0).getFileListMarker());
    }

    @Test
    public void validateDefaultFileListMarkerUnsetAndDataTypeSet() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setFileListMarker("set");
        flagMakerConfig.getDefaultCfg().setFileListMarker(null);
        flagMakerConfig.validate();
        assertEquals("set", flagMakerConfig.getFlagConfigs().get(0).getFileListMarker());
    }

    @Test
    public void validateAllFileListMarkerUnset() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setFileListMarker(null);
        flagMakerConfig.getDefaultCfg().setFileListMarker(null);
        flagMakerConfig.validate();
        assertNull(flagMakerConfig.getFlagConfigs().get(0).getFileListMarker());
    }

    @Test
    public void validateDefaultFileListMarkerIgnoredWhenSetForDataType() throws IOException {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG));
        flagMakerConfig.getFlagConfigs().get(0).setFileListMarker("set");
        flagMakerConfig.getDefaultCfg().setFileListMarker("default");
        flagMakerConfig.validate();
        assertEquals("set", flagMakerConfig.getFlagConfigs().get(0).getFileListMarker());
    }

    public static class NoOpFlagDistributor implements FlagDistributor {

        @Override
        public void loadFiles(FlagDataTypeConfig fc) {

        }

        @Override
        public boolean hasNext(boolean mustHaveMax) {
            return false;
        }

        @Override
        public Collection<InputFile> next(SizeValidator validator) {
            return null;
        }
    }
}
