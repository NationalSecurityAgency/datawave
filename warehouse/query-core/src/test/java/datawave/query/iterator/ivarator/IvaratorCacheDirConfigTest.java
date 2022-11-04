package datawave.query.iterator.ivarator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IvaratorCacheDirConfigTest {
    
    @Test
    public void jsonSerializationTest() throws IOException {
        List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs = new ArrayList<>();
        
        ivaratorCacheDirConfigs.add(new IvaratorCacheDirConfig("file:/some/path", 0, 1024));
        ivaratorCacheDirConfigs.add(new IvaratorCacheDirConfig("hdfs:/some/other/path", 1, 0.5));
        
        String json = IvaratorCacheDirConfig.toJson(ivaratorCacheDirConfigs);
        List<IvaratorCacheDirConfig> parsedConfigs = IvaratorCacheDirConfig.fromJson(json);
        
        Assertions.assertNotNull(parsedConfigs);
        Assertions.assertEquals(2, parsedConfigs.size());
        
        // ensure order is preserved
        Assertions.assertEquals(ivaratorCacheDirConfigs.get(0), parsedConfigs.get(0));
        Assertions.assertEquals(ivaratorCacheDirConfigs.get(1), parsedConfigs.get(1));
        
        String singleJson = IvaratorCacheDirConfig.toJson(ivaratorCacheDirConfigs.get(0));
        parsedConfigs = IvaratorCacheDirConfig.fromJson(singleJson);
        
        Assertions.assertNotNull(parsedConfigs);
        Assertions.assertEquals(1, parsedConfigs.size());
        Assertions.assertEquals(ivaratorCacheDirConfigs.get(0), parsedConfigs.get(0));
        
        String moreJson = "[{}]";
        List<IvaratorCacheDirConfig> theConfigs = IvaratorCacheDirConfig.fromJson(moreJson);
        
        Assertions.assertEquals(1, theConfigs.size());
        IvaratorCacheDirConfig config = theConfigs.get(0);
        
        Assertions.assertNull(config.getBasePathURI());
        Assertions.assertEquals(IvaratorCacheDirConfig.DEFAULT_PRIORITY, config.getPriority());
        Assertions.assertEquals(IvaratorCacheDirConfig.DEFAULT_MIN_AVAILABLE_STORAGE_MiB, config.getMinAvailableStorageMiB());
        Assertions.assertEquals(IvaratorCacheDirConfig.DEFAULT_MIN_AVAILABLE_STORAGE_PERCENT, config.getMinAvailableStoragePercent(), 0.0);
    }
}
