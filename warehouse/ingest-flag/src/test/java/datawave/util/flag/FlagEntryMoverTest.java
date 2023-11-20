package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Also see FlagEntryMoverParameterizedTest. FlagEntryMoverTest demonstrates additional edge cases not already captured through the parameterized test.
 */
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
        testFileGenerator.withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        testFileGenerator.withTestFlagMakerConfig();
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

    /**
     * This test highlights the behavior when there are two name conflicts (one in flagging and one in flagged) and where the file in flagging has different
     * contents while the file if flagged is identical
     *
     * @throws Exception
     */
    @Test
    public void resolveNameConflictInFlaggingDuplicateInFlagged() throws Exception {
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

        // attempt the move, resulting in a conflict detection leading to inputFile's name change
        InputFile result = flagEntryMover.call();

        assertSame(inputFile, result);

        // should have moved from input to flagging (with the new name)
        assertTrue(inputFile.isMoved());
        assertEquals(inputFile.getFlagging(), inputFile.getCurrentDir());
        assertFalse(fileSystem.exists(inputFile.getPath()));
        assertTrue(fileSystem.exists(inputFile.getFlagging()));
        assertFalse(fileSystem.exists(inputFile.getFlagged()));
        assertFalse(fileSystem.exists(inputFile.getLoaded()));

        // should have changed its name due to the conflict
        assertFileNameChanged(inputFile, result);

        // the original non-duplicate file with the same name is still there
        assertTrue(fileSystem.exists(flaggingBeforeRename));
        // the original duplicate file in flagged is still there
        assertTrue(fileSystem.exists(flaggedBeforeRename));
        assertFalse(fileSystem.exists(loadedBeforeRename));
    }

    /**
     * This test highlights the behavior when there are two name conflicts (one in flagging and one in flagged) and where the file in flagging has the same
     * contents while the file in flagged does not
     *
     * @throws Exception
     */
    @Test
    public void resolveDuplicateInFlaggingNameConflictInFlagged() throws Exception {
        // create a file with the same name but different contents in flagging (should allow a move but to a new name)
        // and create a duplicate in flagged (ignored)
        Path flaggingBeforeRename = inputFile.getFlagging();
        Path flaggedBeforeRename = inputFile.getFlagged();
        Path loadedBeforeRename = inputFile.getLoaded();

        fileSystem.copyFromLocalFile(inputFilePath, flaggingBeforeRename);
        createFileWithDifferentChecksum(this.fileSystem, flaggedBeforeRename);

        assertTrue(fileSystem.exists(inputFile.getFlagging()));
        assertTrue(fileSystem.exists(inputFile.getFlagged()));
        assertFalse(fileSystem.exists(inputFile.getLoaded()));

        // attempt the move, resulting in a conflict detection leading to inputFile's name change
        InputFile result = flagEntryMover.call();

        assertSame(inputFile, result);

        // should have removed the inputFile because it's a duplicate of a file in flagging
        assertFalse(inputFile.isMoved());
        // the currentDir remains in place
        assertEquals(inputFile.getPath(), inputFile.getCurrentDir());
        assertFalse(fileSystem.exists(inputFile.getPath()));
        // the other duplicates go unchanged
        assertTrue(fileSystem.exists(inputFile.getFlagging()));
        assertTrue(fileSystem.exists(inputFile.getFlagged()));
        assertFalse(fileSystem.exists(inputFile.getLoaded()));

        // should have changed its name due to the conflict
        assertEquals(flaggingBeforeRename.getName(), inputFile.getFileName());

        // the original duplicate file with the same name is still there
        assertTrue(fileSystem.exists(flaggingBeforeRename));
        // the original non-duplicate file in flagged is still there
        assertTrue(fileSystem.exists(flaggedBeforeRename));
        assertFalse(fileSystem.exists(loadedBeforeRename));
    }

    static void assertFileNameChanged(InputFile inputFile, InputFile result) {
        // the original inputFile should now exist in the tracked directory
        // expected flagging but was flagged
        assertEquals(inputFile.getFlagging(), result.getCurrentDir());
        assertEquals(inputFile.getFlagging(), inputFile.getCurrentDir());

        // the result of the move should now have a different name
        final String originalName = result.getPath().getName();

        // the flagged name should be modified from the original
        final String modifiedName = result.getFlagging().getName();
        assertNotEquals(originalName, modifiedName);

        // the flagging and loaded names should match the modified name
        assertEquals(modifiedName, result.getFlagging().getName());
        assertEquals(modifiedName, result.getLoaded().getName());
    }

    static void createFileWithDifferentChecksum(FileSystem fileSystem, Path path) throws InterruptedException, IOException {
        Thread.sleep(1);
        try (final OutputStream os = fileSystem.create(path)) {
            os.write(("" + System.currentTimeMillis()).getBytes());
        }
    }
}
