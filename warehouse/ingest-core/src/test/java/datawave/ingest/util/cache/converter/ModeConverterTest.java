package datawave.ingest.util.cache.converter;

import datawave.ingest.util.cache.converter.ModeConverter;
import org.junit.Assert;
import org.junit.Test;

import static datawave.ingest.util.cache.mode.LoadJobCacheMode.Mode.CLASSPATH;
import static datawave.ingest.util.cache.mode.LoadJobCacheMode.Mode.FILE_PATTERN;

public class ModeConverterTest {
    private static final ModeConverter MODE_CONVERTER = new ModeConverter();
    
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
