package datawave.util.flag.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import javax.xml.bind.JAXBException;

import org.junit.Test;

public class FlagDataTypeConfigTest {
    @Test
    public void deserializeWithFewDefaults() throws IOException {
        // @formatter:off
        String content = "<defaultCfg>\n" + "    <maxFlags>10</maxFlags>\n"
                + "    <reducers>10</reducers>\n"
                + "    <script>bin/ingest/bulk-ingest.sh</script>\n"
                + "</defaultCfg>\n";
        // @formatter:on

        FlagDataTypeConfig flagDataTypeConfig = FlagMakerConfigUtility.getXmlObject(FlagDataTypeConfig.class, new StringReader(content));

        FlagDataTypeConfig expectedConfig = new FlagDataTypeConfig();
        expectedConfig.setMaxFlags(10);
        expectedConfig.setReducers(10);
        expectedConfig.setScript("bin/ingest/bulk-ingest.sh");

        assertEquals(expectedConfig, flagDataTypeConfig);
    }

    @Test
    public void declareExpectedDefaults() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();

        // explicitly assert each default
        assertEquals(0, flagDataTypeConfig.getMaxFlags());
        assertEquals(0, flagDataTypeConfig.getReducers());
        assertNull(flagDataTypeConfig.getScript());
        assertNull(flagDataTypeConfig.getDataName());
        assertNull(flagDataTypeConfig.getFolder());
        assertEquals(datawave.ingest.input.reader.event.EventSequenceFileInputFormat.class, flagDataTypeConfig.getInputFormat());
        assertNull(flagDataTypeConfig.getIngestPool());
        assertNull(flagDataTypeConfig.getExtraIngestArgs());
        assertEquals("none", flagDataTypeConfig.getDistributionArgs());
        assertEquals(Integer.MIN_VALUE, flagDataTypeConfig.getTimeoutMilliSecs());
        assertEquals(Integer.MIN_VALUE, flagDataTypeConfig.getFlagCountThreshold());
        assertFalse(flagDataTypeConfig.isLifo());
        assertNull(flagDataTypeConfig.getFileListMarker());
        assertNull(flagDataTypeConfig.getCollectMetrics());
    }

    @Test
    public void serializeDefaultFlagDataTypeConfig() throws JAXBException {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        FlagMakerConfigUtility.saveXmlObject(flagDataTypeConfig, outputStream);
        String serializedDataTypeConfig = outputStream.toString();
        assertFalse("Unexpected contents: " + serializedDataTypeConfig, serializedDataTypeConfig.contains("<last>"));

        // @formatter:off
        String expectedContent = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                + "<flagDataTypeConfig>\n"
                + "    <distributionArgs>none</distributionArgs>\n"
                + "    <flagCountThreshold>-2147483648</flagCountThreshold>\n"
                + "    <inputFormat>datawave.ingest.input.reader.event.EventSequenceFileInputFormat</inputFormat>\n"
                + "    <lifo>false</lifo>\n"
                + "    <maxFlags>0</maxFlags>\n"
                + "    <reducers>0</reducers>\n"
                + "    <timeoutMilliSecs>-2147483648</timeoutMilliSecs>\n"
                + "</flagDataTypeConfig>\n";
        // @formatter:on
        assertEquals("Unexpected contents: " + serializedDataTypeConfig, expectedContent, serializedDataTypeConfig);
    }

    @Test
    public void deserializedConfigMatchesDefaultConstructor() throws IOException {
        FlagDataTypeConfig flagDataTypeConfig = FlagMakerConfigUtility.getXmlObject(FlagDataTypeConfig.class, new StringReader("<emptyCfg/>"));
        assertEquals(new FlagDataTypeConfig(), flagDataTypeConfig);
    }

    /**
     * Folder contains a comma separated list of folders. Transition to having a configurable List instead of a String
     */
    @Test
    public void setFolderOverridesFolders() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();
        flagDataTypeConfig.setFolders(Arrays.asList("four", "five"));
        flagDataTypeConfig.setFolder("one,two,three");
        assertEquals(Arrays.asList("one", "two", "three"), flagDataTypeConfig.getFolders());
    }

    @Test
    public void setFoldersOverridesFolder() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();
        flagDataTypeConfig.setFolder("four,five");
        flagDataTypeConfig.setFolders(Arrays.asList("one", "two", "three"));
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());
    }

    @Test
    public void setFolderUpdatesFolder() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();
        assertEquals(null, flagDataTypeConfig.getFolder());

        flagDataTypeConfig.setFolder("four,five");
        assertEquals("four,five", flagDataTypeConfig.getFolder());

        flagDataTypeConfig.setFolder("one,two,three");
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());
    }

    @Test
    public void setFoldersUpdatesFolders() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();
        assertEquals(null, flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setFolders(Arrays.asList("four", "five"));
        assertEquals(Arrays.asList("four", "five"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setFolders(Arrays.asList("one", "two", "three"));
        assertEquals(Arrays.asList("one", "two", "three"), flagDataTypeConfig.getFolders());
    }

    @Test
    public void setFolderOverridesFoldersWithSetBaseHdfs() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();

        flagDataTypeConfig.setBaseHdfsDir("counting");
        flagDataTypeConfig.setFolders(Arrays.asList("four", "five"));
        assertEquals(Arrays.asList("counting/four", "counting/five"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("summation");
        flagDataTypeConfig.setFolder("one,two,three");
        assertEquals(Arrays.asList("summation/one", "summation/two", "summation/three"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("numbers");
        assertEquals(Arrays.asList("numbers/one", "numbers/two", "numbers/three"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("words");
        assertEquals(Arrays.asList("words/one", "words/two", "words/three"), flagDataTypeConfig.getFolders());
    }

    @Test
    public void setFoldersOverridesFolderWithSetBaseHdfs() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();

        flagDataTypeConfig.setBaseHdfsDir("counting");
        flagDataTypeConfig.setFolder("four,five");
        assertEquals("four,five", flagDataTypeConfig.getFolder());

        flagDataTypeConfig.setBaseHdfsDir("summation");
        flagDataTypeConfig.setFolders(Arrays.asList("one", "two", "three"));
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());

        flagDataTypeConfig.setBaseHdfsDir("numbers");
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());
    }

    @Test
    public void setFolderUpdatesFolderWithSetBaseHdfs() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();

        flagDataTypeConfig.setBaseHdfsDir("counting");
        assertEquals(null, flagDataTypeConfig.getFolder());

        flagDataTypeConfig.setBaseHdfsDir("summation");
        flagDataTypeConfig.setFolder("four,five");
        assertEquals("four,five", flagDataTypeConfig.getFolder());
        assertEquals(Arrays.asList("summation/four", "summation/five"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("numbers");
        flagDataTypeConfig.setFolder("one,two,three");
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());
        assertEquals(Arrays.asList("numbers/one", "numbers/two", "numbers/three"), flagDataTypeConfig.getFolders());
    }

    @Test
    public void setFoldersUpdatesFoldersWithSetBaseHdfs() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();

        flagDataTypeConfig.setBaseHdfsDir("counting");
        assertEquals(null, flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("summation");
        flagDataTypeConfig.setFolders(Arrays.asList("four", "five"));
        assertEquals("four,five", flagDataTypeConfig.getFolder());
        assertEquals(Arrays.asList("summation/four", "summation/five"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("numbers");
        flagDataTypeConfig.setFolders(Arrays.asList("one", "two", "three"));
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());
        assertEquals(Arrays.asList("numbers/one", "numbers/two", "numbers/three"), flagDataTypeConfig.getFolders());
    }

    @Test
    public void onlyHonorMostRecentSetBaseHdfsDirCall() {
        FlagDataTypeConfig flagDataTypeConfig = new FlagDataTypeConfig();

        flagDataTypeConfig.setBaseHdfsDir("counting");
        flagDataTypeConfig.setBaseHdfsDir("summation");
        assertEquals(null, flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("counting");
        flagDataTypeConfig.setBaseHdfsDir("summation");
        flagDataTypeConfig.setFolders(Arrays.asList("four", "five"));
        assertEquals("four,five", flagDataTypeConfig.getFolder());
        assertEquals(Arrays.asList("summation/four", "summation/five"), flagDataTypeConfig.getFolders());

        flagDataTypeConfig.setBaseHdfsDir("counting");
        flagDataTypeConfig.setBaseHdfsDir("summation");
        flagDataTypeConfig.setBaseHdfsDir("numbers");
        flagDataTypeConfig.setFolders(Arrays.asList("one", "two", "three"));
        assertEquals("one,two,three", flagDataTypeConfig.getFolder());
        assertEquals(Arrays.asList("numbers/one", "numbers/two", "numbers/three"), flagDataTypeConfig.getFolders());
    }
}
