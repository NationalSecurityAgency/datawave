package datawave.util.flag;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 *
 */
public class FlagEntryMoverTest extends AbstractFlagConfig {
    
    private static final Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
                    .concurrencyLevel(10).build();
    private FileSystem fs;
    
    public FlagEntryMoverTest() {}
    
    @Before
    public void before() throws Exception {
        fmc = getDefaultFMC();
        fmc.setBaseHDFSDir(fmc.getBaseHDFSDir().replace("target", "target/FlagEntryMoverTest"));
        fs = FileSystem.getLocal(new Configuration());
        
        cleanTestDirs();
    }
    
    @Test
    public void testWrite() throws IOException {
        final String METRICS_DIR = "target/test/metrics";
        
        FlagMetrics metrics = new FlagMetrics(this.fs, false);
        metrics.updateCounter(this.getClass().getSimpleName(), "COUNTER_ONE", System.currentTimeMillis());
        metrics.writeMetrics(METRICS_DIR, "base");
        
    }
    
    /**
     * Test of call method, of class FlagEntryMover.
     */
    @Test
    public void testCall() throws Exception {
        
        Path file = getTestFile(fs);
        
        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        createTrackedDirs(fs, entry);
        
        FlagEntryMover instance = new FlagEntryMover(directoryCache, fs, entry);
        InputFile result = instance.call();
        assertSame(result, entry);
        assertTrue(result.isMoved());
        Assert.assertEquals(result.getFlagging(), result.getCurrentDir());
        Assert.assertEquals(entry.getPath().getName(), entry.getFlagged().getName());
        Assert.assertEquals(entry.getPath().getName(), entry.getFlagging().getName());
        Assert.assertEquals(entry.getPath().getName(), entry.getLoaded().getName());
    }
    
    @Test
    public void testConflict() throws Exception {
        
        Path file = getTestFile(fs);
        
        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        createTrackedDirs(fs, entry);
        
        fs.copyFromLocalFile(file, entry.getFlagging());
        
        FlagEntryMover instance = new FlagEntryMover(directoryCache, fs, entry);
        InputFile result = instance.call();
        assertFalse("Should not have moved", result.isMoved());
        Assert.assertEquals(entry, result);
        Assert.assertEquals(entry.getPath(), result.getCurrentDir());
        Assert.assertEquals(entry.getPath().getName(), entry.getFlagged().getName());
        Assert.assertEquals(entry.getPath().getName(), entry.getFlagging().getName());
        Assert.assertEquals(entry.getPath().getName(), entry.getLoaded().getName());
    }
    
    @Test
    public void testConflictMove() throws Exception {
        
        Path file = getTestFile(fs);
        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        createTrackedDirs(fs, entry);
        
        // create conflict file with different checksum
        final Path loadFile = entry.getLoaded();
        Thread.sleep(10);
        try (final OutputStream os = fs.create(loadFile)) {
            os.write(("" + System.currentTimeMillis()).getBytes());
        }
        
        FlagEntryMover instance = new FlagEntryMover(directoryCache, fs, entry);
        final InputFile result = instance.call();
        assertTrue(result.isMoved());
        assertTrue(entry.equals(result));
        // current path should match flagging
        assertNotEquals(result.getPath(), result.getCurrentDir());
        Assert.assertEquals(entry.getFlagging(), result.getCurrentDir());
        // path name should differ from flagged/flagging/loaded
        final String pathName = result.getPath().getName();
        final String flaggedName = result.getFlagged().getName();
        Assert.assertNotEquals(pathName, flaggedName);
        final String flaggingName = result.getFlagging().getName();
        Assert.assertEquals(flaggingName, flaggedName);
        final String loadedName = result.getLoaded().getName();
        Assert.assertEquals(loadedName, flaggedName);
    }
}
