package datawave.util.flag;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

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

public class FlagEntryMoverTest {
    private static final Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
                    .concurrencyLevel(10).build();

    private FileSystem fileSystem;
    private FlagFileTestSetup testFileGenerator;
    private Path inputFilePath;
    private InputFile inputFile;
    private FlagEntryMover flagEntryMover;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void createInputFilesAndMover() throws Exception {
        // set up directories and files
        testFileGenerator = new FlagFileTestSetup();
        fileSystem = testFileGenerator.getFileSystem();
        testFileGenerator.withTestFlagMakerConfig();
        testFileGenerator.withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        testFileGenerator.createTestFiles();

        // identify path to one of the input file that was just created
        inputFilePath = FlagFileTestInspector.getPathToAnyInputFile(fileSystem, testFileGenerator.getFlagMakerConfig());

        // create associated inputFile object
        inputFile = new InputFile("foo", inputFilePath, 0, 0, 0, testFileGenerator.getFlagMakerConfig().getBaseHDFSDir());

        flagEntryMover = new FlagEntryMover(directoryCache, fileSystem, inputFile);
    }

    @After
    public void cleanup() throws IOException {
        testFileGenerator.deleteTestDirectories();
    }

    @Test
    public void resolvesMixedConflicts() throws IOException, InterruptedException {
        // create a file with the same name but different contents in flagging (should allow a move but to a new name)
        // and create a duplicate in flagged (ignored)
        Path flaggingBeforeRename = inputFile.getFlagging();
        Path flaggedBeforeRename = inputFile.getFlagged();
        Path loadedBeforeRename = inputFile.getLoaded();

        createFileWithDifferentChecksum(this.fileSystem, flaggingBeforeRename);
        fileSystem.copyFromLocalFile(inputFilePath, flaggedBeforeRename);

        assertTrue(fileSystem.exists(inputFile.getFlagging()));
        assertTrue(fileSystem.exists(inputFile.getFlagged()));
        assertFalse(fileSystem.exists(inputFile.getLoaded()));

        InputFile result = flagEntryMover.call();

        assertSame(inputFile, result);

        // should have moved from input to flagging
        assertTrue(inputFile.isMoved());
        Assert.assertEquals(inputFile.getFlagging(), inputFile.getCurrentDir());
        assertFalse(fileSystem.exists(inputFile.getPath()));
        assertTrue(fileSystem.exists(inputFile.getFlagging()));
        assertFalse(fileSystem.exists(inputFile.getFlagged()));
        assertFalse(fileSystem.exists(inputFile.getLoaded()));

        // should have changed its name
        assertFileNameChanged(inputFile, result);

        // the original non-duplicate file with the same name is still there
        assertTrue(fileSystem.exists(flaggingBeforeRename));
        // the original duplicate file in flagged is still there
        // TODO - change the behavior here to eliminate the duplicate in flagged?
        assertTrue(fileSystem.exists(flaggedBeforeRename));
        assertFalse(fileSystem.exists(loadedBeforeRename));
    }

    static void assertFileNameChanged(InputFile inputFile, InputFile result) {
        // the original inputFile should now exist in the tracked directory
        // expected flagging but was flagged
        Assert.assertEquals(inputFile.getFlagging(), result.getCurrentDir());
        Assert.assertEquals(inputFile.getFlagging(), inputFile.getCurrentDir());

        // the result of the move should now have a different name
        final String originalName = result.getPath().getName();

        // the flagged name should be modified from the original
        final String modifiedName = result.getFlagging().getName();
        Assert.assertNotEquals(originalName, modifiedName);

        // the flagging and loaded names should match the modified name
        Assert.assertEquals(modifiedName, result.getFlagging().getName());
        Assert.assertEquals(modifiedName, result.getLoaded().getName());
    }

    static void createFileWithDifferentChecksum(FileSystem fileSystem, Path path) throws InterruptedException, IOException {
        Thread.sleep(1);
        try (final OutputStream os = fileSystem.create(path)) {
            os.write(("" + System.currentTimeMillis()).getBytes());
        }
    }
}
