package datawave.util.flag;

import static org.junit.Assert.assertEquals;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import datawave.util.flag.config.FlagMakerConfig;

public class FlagEntryMoverTest {

    private static final Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
                    .concurrencyLevel(10).build();
    private FileSystem fs;
    private FlagMakerConfig fmc;
    private FlagFileTestSetup testFileGenerator;

    public FlagEntryMoverTest() {}

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception {
        testFileGenerator = new FlagFileTestSetup();
        testFileGenerator.withTestFlagMakerConfig().withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        fmc = testFileGenerator.getFlagMakerConfig();
        fs = FileSystem.getLocal(new Configuration());
    }

    @After
    public void cleanup() throws IOException {
        testFileGenerator.deleteTestDirectories();
    }

    /**
     * Test of call method, of class FlagEntryMover.
     */
    @Test
    public void testCall() throws Exception {

        testFileGenerator.createTestFiles();
        Path file = FlagFileTestInspector.getPathToAnyInputFile(fs, fmc);

        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        testFileGenerator.createTrackedDirsForInputFile(entry);

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

        testFileGenerator.createTestFiles();
        Path file = FlagFileTestInspector.getPathToAnyInputFile(fs, fmc);

        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        testFileGenerator.createTrackedDirsForInputFile(entry);

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

        testFileGenerator.createTestFiles();
        Path file = FlagFileTestInspector.getPathToAnyInputFile(fs, fmc);

        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        testFileGenerator.createTrackedDirsForInputFile(entry);

        // create conflict file with different checksum
        final Path loadFile = entry.getLoaded();
        Thread.sleep(10);
        try (final OutputStream os = fs.create(loadFile)) {
            os.write(("" + System.currentTimeMillis()).getBytes());
        }

        FlagEntryMover instance = new FlagEntryMover(directoryCache, fs, entry);
        final InputFile result = instance.call();
        assertTrue(result.isMoved());
        assertEquals(entry, result);
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
