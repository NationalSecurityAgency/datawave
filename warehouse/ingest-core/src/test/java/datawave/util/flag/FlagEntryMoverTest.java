package datawave.util.flag;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Parameterized test
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
        testFileGenerator.withTestFlagMakerConfig();
        testFileGenerator.withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        testFileGenerator.createTestFiles();

        // identify path to one of the input file that was just created
        inputFilePath = FlagFileTestInspector.getPathToAnyInputFile(fileSystem, testFileGenerator.getFlagMakerConfig());

        // create associated inputFile object
        inputFile = new InputFile("foo", inputFilePath, 0, 0, 0, testFileGenerator.getFlagMakerConfig().getBaseHDFSDir());

        flagEntryMover = new FlagEntryMover(directoryCache, fileSystem, inputFile);
    }

    @Test
    public void resolvesMixedConflicts() throws IOException, InterruptedException {
        // create a file with the same name but different contents in flagging (should allow a move but to a new name) TODO, does this undermine the cleanup?
        // and a create a duplicate in flagged (should result in deleting the original and NOT doing a move)
        // should remove the input file
        // should leave the duplicate intact
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

        // should have moved to flagging
        assertTrue(inputFile.isMoved());
        Assert.assertEquals(inputFile.getFlagging(), inputFile.getCurrentDir());
        assertFalse(fileSystem.exists(inputFile.getPath()));
        assertTrue(fileSystem.exists(inputFile.getFlagging()));
        assertFalse(fileSystem.exists(inputFile.getFlagged()));
        assertFalse(fileSystem.exists(inputFile.getLoaded()));


        assertFileNameChanged(result);

        assertTrue(fileSystem.exists(flaggingBeforeRename));
        // TODO - change the behavior here to eliminate the duplicate in flagged?
        assertTrue(fileSystem.exists(flaggedBeforeRename));
        assertFalse(fileSystem.exists(loadedBeforeRename));
    }

    static void createFileWithDifferentChecksum(FileSystem fileSystem, Path path) throws InterruptedException, IOException {
        Thread.sleep(1);
        try (final OutputStream os = fileSystem.create(path)) {
            os.write(("" + System.currentTimeMillis()).getBytes());
        }
    }

    @After
    public void cleanup() throws IOException {
        testFileGenerator.deleteTestDirectories();
    }

// todo - add a test for multiple conflicts
    private void assertFileNameChanged(InputFile result) {
        // the original inputFile should now exist in the tracked directory
        // expected flagging but was flagged
//        Assert.assertEquals(inputFile.getCurrentDir(), this.tra()); TODO
        Assert.assertEquals(inputFile.getCurrentDir(), result.getCurrentDir());

        // the result of the move should now have a different name
        final String originalName = result.getPath().getName();

        // the flagged name should be modified from the original
        final String modifiedName = result.getFlagged().getName();
        Assert.assertNotEquals(originalName, modifiedName);

        // the flagging and loaded names should match the modified name
        Assert.assertEquals(modifiedName, result.getFlagging().getName());
        Assert.assertEquals(modifiedName, result.getLoaded().getName());
    }

//
//    @Test
//    public void deletesFileWhenChecksumMatchesFileInFlagging() throws Exception {
//        Path pathToOriginal = inputFile.getPath();
//        Path pathToCopy = inputFile.getFlagging();
//
//        assertTrue(fileSystem.exists(pathToOriginal));
//        assertFalse(fileSystem.exists(pathToCopy));
//
//        createCopyOfFileInFlagging();
//        assertTrue(fileSystem.exists(pathToOriginal));
//        assertTrue(fileSystem.exists(pathToCopy));
//
//        instance.call();
//        assertFalse(fileSystem.exists(pathToOriginal));
//        assertTrue(fileSystem.exists(pathToCopy));
//    }
//
//    @Test
//    public void testConflictMove() throws Exception {
//        createDifferentFileWithSameNameInLoaded();
//
//        final InputFile result = instance.call();
//        // current path should match flagging
//        assertNotEquals(result.getPath(), result.getCurrentDir());
//        Assert.assertEquals(inputFile.getFlagging(), result.getCurrentDir());
//        // path name should differ from flagged/flagging/loaded
//        final String pathName = result.getPath().getName();
//        final String flaggedName = result.getFlagged().getName();
//        Assert.assertNotEquals(pathName, flaggedName);
//        final String flaggingName = result.getFlagging().getName();
//        Assert.assertEquals(flaggingName, flaggedName);
//        final String loadedName = result.getLoaded().getName();
//        Assert.assertEquals(loadedName, flaggedName);
//    }
//

    private void assertFileNameConsistency() {
        Assert.assertEquals(inputFilePath.getName(), inputFile.getFlagged().getName());
        Assert.assertEquals(inputFilePath.getName(), inputFile.getFlagging().getName());
        Assert.assertEquals(inputFilePath.getName(), inputFile.getLoaded().getName());
    }
//
//    @Test
//    public void reportsOriginalLocationAfterCollisionWithCopy() throws Exception {
//        createCopyOfFileInFlagging();
//        InputFile result = instance.call();
//        Assert.assertEquals(inputFile.getPath(), result.getCurrentDir());
//    }
//
//    @Test
//    public void reportsFlaggingLocationAfterCollisionInLoadedWithName() throws Exception {
//        createDifferentFileWithSameNameInLoaded();
//
//        final InputFile result = instance.call();
//        // current path should match flagging
//        assertNotEquals(result.getPath(), result.getCurrentDir());
//        Assert.assertEquals(inputFile.getFlagging(), result.getCurrentDir());
//    }
}
