package datawave.ingest.util.cache.converter;

import org.junit.Assert;
import org.junit.Test;

import static datawave.ingest.util.cache.lease.JobCacheLockFactory.Mode.NO_OP;
import static datawave.ingest.util.cache.lease.JobCacheLockFactory.Mode.ZOOKEEPER;

public class CacheLockConverterTest {
    private static final CacheLockConverter LOCK_CONVERTER = new CacheLockConverter();
    
    @Test
    public void testZookeeperMode() {
        Assert.assertEquals(LOCK_CONVERTER.convert(ZOOKEEPER.name()).getMode(), ZOOKEEPER);
    }
    
    @Test
    public void testNoOpMode() {
        Assert.assertEquals(LOCK_CONVERTER.convert(NO_OP.name()).getMode(), NO_OP);
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMode() {
        LOCK_CONVERTER.convert("INVALID");
    }
}
