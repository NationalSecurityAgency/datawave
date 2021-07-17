package datawave.ingest.util.cache.converter;

import org.junit.Assert;
import org.junit.Test;

import static datawave.ingest.util.cache.load.mode.LoadJobCacheMode.Mode.CLASSPATH;
import static datawave.ingest.util.cache.load.mode.LoadJobCacheMode.Mode.FILE_PATTERN;

public class LoadModeConverterTest {
    private static final LoadModeConverter MODE_CONVERTER = new LoadModeConverter();
    
    @Test
    public void testClasspathMode() {
        Assert.assertEquals(MODE_CONVERTER.convert(CLASSPATH.name()).getMode(), CLASSPATH);
    }
    
    @Test
    public void testFilePatternMode() {
        Assert.assertEquals(MODE_CONVERTER.convert(FILE_PATTERN.name()).getMode(), FILE_PATTERN);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMode() {
        MODE_CONVERTER.convert("INVALID");
    }
}
