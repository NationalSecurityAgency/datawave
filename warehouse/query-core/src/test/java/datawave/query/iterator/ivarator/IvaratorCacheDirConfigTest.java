package datawave.query.iterator.ivarator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

public class IvaratorCacheDirConfigTest {

    @Test
    public void jsonSerializationTest() throws IOException {
        List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs = new ArrayList<>();

        ivaratorCacheDirConfigs.add(new IvaratorCacheDirConfig("file:/some/path", 0, 1024));
        ivaratorCacheDirConfigs.add(new IvaratorCacheDirConfig("hdfs:/some/other/path", 1, 0.5));

        String json = IvaratorCacheDirConfig.toJson(ivaratorCacheDirConfigs);
        List<IvaratorCacheDirConfig> parsedConfigs = IvaratorCacheDirConfig.fromJson(json);

        assertNotNull(parsedConfigs);
        assertEquals(2, parsedConfigs.size());

        // ensure order is preserved
        assertEquals(ivaratorCacheDirConfigs.get(0), parsedConfigs.get(0));
        assertEquals(ivaratorCacheDirConfigs.get(1), parsedConfigs.get(1));

        String singleJson = IvaratorCacheDirConfig.toJson(ivaratorCacheDirConfigs.get(0));
        parsedConfigs = IvaratorCacheDirConfig.fromJson(singleJson);

        assertNotNull(parsedConfigs);
        assertEquals(1, parsedConfigs.size());
        assertEquals(ivaratorCacheDirConfigs.get(0), parsedConfigs.get(0));

        String moreJson = "[{}]";
        List<IvaratorCacheDirConfig> theConfigs = IvaratorCacheDirConfig.fromJson(moreJson);

        assertEquals(1, theConfigs.size());
        IvaratorCacheDirConfig config = theConfigs.get(0);

        assertNull(config.getBasePathURI());
        assertEquals(IvaratorCacheDirConfig.DEFAULT_PRIORITY, config.getPriority());
        assertEquals(IvaratorCacheDirConfig.DEFAULT_MIN_AVAILABLE_STORAGE_MiB, config.getMinAvailableStorageMiB());
        assertEquals(IvaratorCacheDirConfig.DEFAULT_MIN_AVAILABLE_STORAGE_PERCENT, config.getMinAvailableStoragePercent(), 0.0);
    }
}
