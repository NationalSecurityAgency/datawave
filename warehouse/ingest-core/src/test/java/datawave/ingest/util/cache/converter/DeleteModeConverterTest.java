package datawave.ingest.util.cache.converter;

import org.junit.Assert;
import org.junit.Test;

import static datawave.ingest.util.cache.delete.mode.DeleteJobCacheMode.Mode.DELETES_SPECIFIED;
import static datawave.ingest.util.cache.delete.mode.DeleteJobCacheMode.Mode.OLD_INACTIVE;

public class DeleteModeConverterTest {
    private static final DeleteModeConverter MODE_CONVERTER = new DeleteModeConverter();
    
    @Test
    public void testDeletesSpecifiedMode() {
        Assert.assertEquals(MODE_CONVERTER.convert(DELETES_SPECIFIED.name()).getMode(), DELETES_SPECIFIED);
    }
    
    @Test
    public void testOldInactiveMode() {
        Assert.assertEquals(MODE_CONVERTER.convert(OLD_INACTIVE.name()).getMode(), OLD_INACTIVE);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMode() {
        MODE_CONVERTER.convert("INVALID");
    }
}
