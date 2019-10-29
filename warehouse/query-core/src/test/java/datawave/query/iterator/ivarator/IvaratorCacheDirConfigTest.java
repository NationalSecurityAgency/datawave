package datawave.query.iterator.ivarator;

import org.junit.Assert;
import org.junit.Test;

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
        
        Assert.assertNotNull(parsedConfigs);
        Assert.assertEquals(2, parsedConfigs.size());
        
        // ensure order is preserved
        Assert.assertEquals(ivaratorCacheDirConfigs.get(0), parsedConfigs.get(0));
        Assert.assertEquals(ivaratorCacheDirConfigs.get(1), parsedConfigs.get(1));
        
        String singleJson = IvaratorCacheDirConfig.toJson(ivaratorCacheDirConfigs.get(0));
        parsedConfigs = IvaratorCacheDirConfig.fromJson(singleJson);
        
        Assert.assertNotNull(parsedConfigs);
        Assert.assertEquals(1, parsedConfigs.size());
        Assert.assertEquals(ivaratorCacheDirConfigs.get(0), parsedConfigs.get(0));
        
        String moreJson = "[{}]";
        List<IvaratorCacheDirConfig> theConfigs = IvaratorCacheDirConfig.fromJson(moreJson);
        
        Assert.assertEquals(1, theConfigs.size());
        IvaratorCacheDirConfig config = theConfigs.get(0);
        
        Assert.assertNull(config.getBasePathURI());
        Assert.assertEquals(IvaratorCacheDirConfig.DEFAULT_PRIORITY, config.getPriority());
        Assert.assertEquals(IvaratorCacheDirConfig.DEFAULT_MIN_AVAILABLE_STORAGE_MB, config.getMinAvailableStorageMB());
        Assert.assertEquals(IvaratorCacheDirConfig.DEFAULT_MIN_AVAILABLE_STORAGE_PERCENT, config.getMinAvailableStoragePercent(), 0.0);
    }
}
