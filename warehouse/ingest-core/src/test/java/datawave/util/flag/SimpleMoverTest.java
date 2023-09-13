package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import datawave.util.flag.config.FlagMakerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import datawave.util.flag.InputFile.TrackedDir;

/**
 *
 */
public class SimpleMoverTest {
    private FlagFileTestSetup testFileGenerator;

    private static final Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
                    .concurrencyLevel(10).build();
    private FlagMakerConfig fmc;

    @Before
    public void before() throws Exception {
        testFileGenerator = new FlagFileTestSetup();
        testFileGenerator.withTestFlagMakerConfig();
        this.fmc = testFileGenerator.fmc;
        this.fmc.setBaseHDFSDir(this.fmc.getBaseHDFSDir().replace("target", "target/SimpleMoverTest"));

        // cleanup flagging and flagged directories
        testFileGenerator.withConfig(fmc);
    }

    @After
    public void cleanup() throws IOException {
        testFileGenerator.deleteTestDirectories();
    }

    /**
     * Test of call method, of class SimpleMover.
     */
    @Test
    public void testCall() throws Exception {

        FileSystem fs = FileSystem.getLocal(new Configuration());

        testFileGenerator.withConfig(fmc).createTestFiles();
        Path file = FlagFileTestHelper.getPathToAnyInputFile(fs, fmc);
        InputFile inFile = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());

        SimpleMover instance = new SimpleMover(directoryCache, inFile, TrackedDir.FLAGGED_DIR, fs);
        InputFile result = instance.call();
        assertTrue("Should have moved to flagged", result.isMoved());
        assertEquals(result.getCurrentDir(), result.getFlagged());
    }

    /**
     * Test of checkParent method, of class SimpleMover.
     */
    @Test
    public void testFailCall() throws Exception {

        FileSystem fs = FileSystem.getLocal(new Configuration());

        testFileGenerator.withConfig(fmc).createTestFiles();
        Path file = FlagFileTestHelper.getPathToAnyInputFile(fs, fmc);
        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());

        // copy to the move will fail
        fs.mkdirs(entry.getFlagging().getParent());
        fs.copyFromLocalFile(file, entry.getFlagging());

        SimpleMover instance = new SimpleMover(directoryCache, entry, TrackedDir.FLAGGING_DIR, fs);

        InputFile result = instance.call();
        assertFalse("should not have moved due to collision", result.isMoved());
        assertEquals(result.getCurrentDir(), result.getFlagged());
    }

}
