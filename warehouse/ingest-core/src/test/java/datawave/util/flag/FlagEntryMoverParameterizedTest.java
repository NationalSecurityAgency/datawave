package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Parameterized test
 */
@RunWith(Parameterized.class)
public class FlagEntryMoverParameterizedTest {
    private static final Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
            .concurrencyLevel(10).build();

    private final CollisionType collisionType;
    private final InputFile.TrackedDir trackedDir;

    private FileSystem fileSystem;
    private FlagFileTestSetup testFileGenerator;
    private Path inputFilePath;
    private InputFile inputFile;
    private FlagEntryMover flagEntryMover;

    @Rule
    public TestName testName = new TestName();

    enum CollisionType {
        BOTH_FILENAME_AND_CHECKSUM_MATCH,
        SAME_FILENAME_DIFFERENT_CHECKSUM,
        NONE
    }

    public FlagEntryMoverParameterizedTest(CollisionType collisionType, InputFile.TrackedDir trackedDir) {
        this.trackedDir = trackedDir;
        this.collisionType = collisionType;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Iterable<Object[]> createTestCases() {
        List<CollisionType> collisionTypes = Arrays.asList(CollisionType.values());
        List<InputFile.TrackedDir> trackedDirectories = Arrays.asList(InputFile.TrackedDir.values());

        List<List<Object>> allPermutations = Lists.cartesianProduct(collisionTypes, trackedDirectories);

        // convert List of Lists to List of Arrays
        return allPermutations.stream().map(list -> {
            return new Object[]{list.get(0), list.get(1)};
        }).collect(Collectors.toList());
    }

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

    @Before
    public void setupFileForCollision() throws IOException, InterruptedException {
        if (this.collisionType == CollisionType.NONE) {
            return;
        }
        // Create path for this test case's tracked directory
        Path locationForCollision = getLocationForCollision();
        if (locationForCollision == null) {
            return;
        }
        switch (this.collisionType) {
            case BOTH_FILENAME_AND_CHECKSUM_MATCH:
                fileSystem.copyFromLocalFile(inputFilePath, locationForCollision);
                break;
            case SAME_FILENAME_DIFFERENT_CHECKSUM:
                FlagEntryMoverTest.createFileWithDifferentChecksum(this.fileSystem, locationForCollision);
                break;
            default:
                break;
        }
    }

    private Path getLocationForCollision() {
        switch (this.trackedDir) {
            case FLAGGING_DIR:
                return inputFile.getFlagging();
            case FLAGGED_DIR:
                return inputFile.getFlagged();
            case LOADED_DIR:
                return inputFile.getLoaded();
            default:
                return null;
        }
    }

    @After
    public void cleanup() throws IOException {
        testFileGenerator.deleteTestDirectories();
    }

    @Test
    public void returnsSameInputFileObject() throws Exception {
        InputFile result = flagEntryMover.call();
        assertSame(inputFile, result);
    }

    @Test
    public void updatesIsMovedStatus() throws Exception {
        assertFalse(inputFile.isMoved());
        flagEntryMover.call();

        if (shouldDetectDuplicateFile()) {
            assertFalse(inputFile.isMoved());
        } else {
            assertTrue(inputFile.isMoved());
        }
    }


    @Test
    public void updatesCurrentDir() throws Exception {
        flagEntryMover.call();
        if (shouldDetectDuplicateFile()) {
            Assert.assertEquals(inputFile.getPath(), inputFile.getCurrentDir());
        } else {
            // todo - why always flagging?
            Assert.assertEquals(inputFile.getFlagging(), inputFile.getCurrentDir());
        }
    }

    private boolean shouldDetectDuplicateFile() {
        boolean shouldBeExactMatch = (this.collisionType == CollisionType.BOTH_FILENAME_AND_CHECKSUM_MATCH);
        boolean isInDifferentDirectory = (this.trackedDir != InputFile.TrackedDir.PATH_DIR);
        return shouldBeExactMatch && isInDifferentDirectory;
    }

    @Test
    public void fileNameMatchesForAllTrackedDirectories() throws IOException {
        assertFileNameConsistency();

        InputFile result = flagEntryMover.call();

        if (shouldRenameFilenameCollision()) {
            assertFileNameChanged(result);
        } else {
            assertFileNameConsistency();
        }
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

    private boolean shouldRenameFilenameCollision() {
        return this.collisionType == CollisionType.SAME_FILENAME_DIFFERENT_CHECKSUM && this.trackedDir != InputFile.TrackedDir.PATH_DIR ;
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
