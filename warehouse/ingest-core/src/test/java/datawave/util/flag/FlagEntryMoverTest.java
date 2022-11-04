package datawave.util.flag;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class FlagEntryMoverTest extends AbstractFlagConfig {
    
    private static final Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
                    .concurrencyLevel(10).build();
    private FileSystem fs;
    
    public FlagEntryMoverTest() {}
    
    @BeforeEach
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
        assertEquals(result.getFlagging(), result.getCurrentDir());
        assertEquals(entry.getPath().getName(), entry.getFlagged().getName());
        assertEquals(entry.getPath().getName(), entry.getFlagging().getName());
        assertEquals(entry.getPath().getName(), entry.getLoaded().getName());
    }
    
    @Test
    public void testConflict() throws Exception {
        
        Path file = getTestFile(fs);
        
        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        createTrackedDirs(fs, entry);
        
        fs.copyFromLocalFile(file, entry.getFlagging());
        
        FlagEntryMover instance = new FlagEntryMover(directoryCache, fs, entry);
        InputFile result = instance.call();
        assertFalse(result.isMoved(), "Should not have moved");
        assertEquals(entry, result);
        assertEquals(entry.getPath(), result.getCurrentDir());
        assertEquals(entry.getPath().getName(), entry.getFlagged().getName());
        assertEquals(entry.getPath().getName(), entry.getFlagging().getName());
        assertEquals(entry.getPath().getName(), entry.getLoaded().getName());
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
        assertEquals(entry.getFlagging(), result.getCurrentDir());
        // path name should differ from flagged/flagging/loaded
        final String pathName = result.getPath().getName();
        final String flaggedName = result.getFlagged().getName();
        assertNotEquals(pathName, flaggedName);
        final String flaggingName = result.getFlagging().getName();
        assertEquals(flaggingName, flaggedName);
        final String loadedName = result.getLoaded().getName();
        assertEquals(loadedName, flaggedName);
    }
}
