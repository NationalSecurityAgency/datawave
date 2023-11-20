package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

/**
 * Also see FlagEntryMoverTest which demonstrates additional edge cases not already captured through the parameterized test.
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
        BOTH_FILENAME_AND_CHECKSUM_MATCH, SAME_FILENAME_DIFFERENT_CHECKSUM, NONE
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
            return new Object[] {list.get(0), list.get(1)};
        }).collect(Collectors.toList());
    }

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

    @After
    public void cleanup() throws IOException {
        testFileGenerator.deleteTestDirectories();
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
    public void updatesCurrentDirUnlessDuplicate() throws Exception {
        flagEntryMover.call();
        if (shouldDetectDuplicateFile()) {
            assertEquals("Current dir should still be input dir for a duplicate", inputFile.getPath(), inputFile.getCurrentDir());
        } else {
            assertEquals("Current dir should move to flagging for non-duplicate", inputFile.getFlagging(), inputFile.getCurrentDir());
        }
    }

    @Test
    public void deletesSrcFileNotDstWhenDuplicate() throws IOException {
        flagEntryMover.call();

        if (shouldDetectDuplicateFile()) {
            assertFalse("Original file should no longer exist", fileSystem.exists(inputFile.getCurrentDir()));
            assertTrue("The duplicate still exists in a destination directory", fileSystem.exists(getLocationForCollision()));
        } else {
            assertTrue("The original file still exists", fileSystem.exists(inputFile.getCurrentDir()));
        }
    }

    @Test
    public void renameFileWhenExpected() throws IOException {
        assertFileNameConsistency();

        InputFile result = flagEntryMover.call();

        if (shouldRenameFilenameCollision()) {
            FlagEntryMoverTest.assertFileNameChanged(inputFile, result);
        } else {
            assertFileNameConsistency();
        }
    }

    private boolean shouldDetectDuplicateFile() {
        boolean shouldBeExactMatch = (this.collisionType == CollisionType.BOTH_FILENAME_AND_CHECKSUM_MATCH);
        boolean isInDifferentDirectory = (this.trackedDir != InputFile.TrackedDir.PATH_DIR);
        return shouldBeExactMatch && isInDifferentDirectory;
    }

    private boolean shouldRenameFilenameCollision() {
        boolean sameNameWithDifferentContents = this.collisionType == CollisionType.SAME_FILENAME_DIFFERENT_CHECKSUM;
        boolean notInputDirectory = this.trackedDir != InputFile.TrackedDir.PATH_DIR;
        return sameNameWithDifferentContents && notInputDirectory;
    }

    private void assertFileNameConsistency() {
        assertEquals(inputFilePath.getName(), inputFile.getFlagged().getName());
        assertEquals(inputFilePath.getName(), inputFile.getFlagging().getName());
        assertEquals(inputFilePath.getName(), inputFile.getLoaded().getName());
    }
}
