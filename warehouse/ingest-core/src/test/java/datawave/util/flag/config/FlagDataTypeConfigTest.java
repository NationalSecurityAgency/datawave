package datawave.util.flag.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;

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
}
