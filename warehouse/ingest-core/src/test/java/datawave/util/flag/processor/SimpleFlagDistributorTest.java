package datawave.util.flag.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.FlagFileTestSetup;
import datawave.util.flag.FlagMakerTest;
import datawave.util.flag.InputFile;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * Also see SimpleFlagDistributorFileMatchingTest which is focused on the pattern matching
 */
public class SimpleFlagDistributorTest {
    private static final Logger LOG = LoggerFactory.getLogger(FlagMakerTest.class);
    private static final SizeValidator SIZE_VALIDATOR = (fc, files) -> true;
    private static final int FULL_BATCH = 10;
    private static final boolean DO_NOT_REQUIRE_MAX_SIZE = false;
    private static final boolean ONLY_FULL_SIZED_FLAGS = true;

    @Rule
    public TestName testName = new TestName();

    private SimpleFlagDistributor simpleFlagDistributor;
    private FlagDataTypeConfig fooAndBarConfig;
    private FlagFileTestSetup flagMakerTestSetup;

    @Before
    public void setUp() throws Exception {
        LOG.error("testName: " + testName.getMethodName());

        flagMakerTestSetup = new FlagFileTestSetup();
        flagMakerTestSetup.withTestFlagMakerConfig().withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        flagMakerTestSetup.getFlagMakerConfig().validate();

        this.simpleFlagDistributor = new SimpleFlagDistributor(flagMakerTestSetup.getFlagMakerConfig());

        this.fooAndBarConfig = flagMakerTestSetup.getFlagMakerConfig().getFlagConfigs().get(0);

        // assumption for multiple test cases
        assertEquals(FULL_BATCH, this.fooAndBarConfig.getMaxFlags());
    }

    @After
    public void cleanup() throws IOException {
        flagMakerTestSetup.deleteTestDirectories();
    }

    @Test
    public void ignoresEmptyDirectories() throws Exception {
        int numDirectories = 9;
        flagMakerTestSetup.withFilesPerDay(0).withNumDays(numDirectories).createTestFiles();

        // verify test setup created directories
        Path pathPattern = new Path(flagMakerTestSetup.getFileSystem().getWorkingDirectory(), new Path(fooAndBarConfig.getFolders().get(0) + "/*/*/*"));
        assertEquals(numDirectories, flagMakerTestSetup.getFileSystem().globStatus(pathPattern).length);

        // verify that nothing is loaded (directories are ignored)
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        assertFalse(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
    }

    @Test
    public void callingNextWhenNothingWasLoadedReturnsNothing() throws Exception {
        // do not create any files
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        simpleFlagDistributor.next(SIZE_VALIDATOR);

        Collection<InputFile> emptyBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(0, emptyBatch.size());
    }

    @Test
    public void testIgnoresMismatchingFilenames() throws Exception {
        // specify strange filename suffix, that will not match configured
        // pattern
        flagMakerTestSetup.withTestFileNameSuffix(".failsFilenamePatternMatching!");
        // load 12 files
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();

        // verify test setup created files
        Path pathPatternFolderOne = new Path(flagMakerTestSetup.getFileSystem().getWorkingDirectory(),
                        new Path(fooAndBarConfig.getFolders().get(0) + "/*/*/*/*"));
        assertEquals(6, flagMakerTestSetup.getFileSystem().globStatus(pathPatternFolderOne).length);
        Path pathPatternFolderTwo = new Path(flagMakerTestSetup.getFileSystem().getWorkingDirectory(),
                        new Path(fooAndBarConfig.getFolders().get(1) + "/*/*/*/*"));
        assertEquals(6, flagMakerTestSetup.getFileSystem().globStatus(pathPatternFolderTwo).length);

        // verify that nothing is loaded (mismatching file names are ignored)
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        assertFalse(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
    }

    @Test
    public void testSimpleUsage() throws Exception {
        // load 12 files
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // get a full batch
        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        Collection<InputFile> files = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(FULL_BATCH, files.size());

        // get a partial batch
        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        Collection<InputFile> secondBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(12 - FULL_BATCH, secondBatch.size());
    }

    @Test
    public void testIgnoresNonexistentFolder() throws Exception {
        // verify that nothing is loaded (the directory was never created)
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        assertFalse(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
    }

    @Test
    public void hasNextOptionallyInsistsOnFullSize() throws Exception {
        // creates 4 files
        flagMakerTestSetup.withFilesPerDay(2).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        assertFalse(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
    }

    @Test
    public void nextReturnsExpectedNumberOfFiles() throws Exception {
        // creates 4 files
        flagMakerTestSetup.withFilesPerDay(2).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        Collection<InputFile> files = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(4, files.size());
    }

    @Test
    public void hasNextReportsNothingLeftAfterNextConsumesFiles() throws Exception {
        // creates 4 files
        flagMakerTestSetup.withFilesPerDay(2).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        simpleFlagDistributor.next(SIZE_VALIDATOR);

        assertFalse(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
        assertFalse(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
    }

    @Test
    public void callingNextAfterConsumingFilesReturnsEmptyBatch() throws Exception {
        // creates 4 files
        flagMakerTestSetup.withFilesPerDay(2).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // consumes the files
        simpleFlagDistributor.next(SIZE_VALIDATOR);

        Collection<InputFile> emptyBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(0, emptyBatch.size());
    }

    @Test
    public void hasNextAccountsForNewlyCreatedFiles() throws Exception {
        // 2 total files = 2 directories * 1 file per directory
        flagMakerTestSetup.withFilesPerDay(1).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        assertFalse(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));

        // not calling next until full

        // 8 additional = 2 directories * 4 file per directory
        flagMakerTestSetup.withFilesPerDay(4).withNumDays(1).createAdditionalTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        // 10 total files = 2 existing + 8 additional

        assertTrue(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
    }

    @Test
    public void nextProvidesFilesAcrossSeparateLoads() throws Exception {
        // 2 total files = 2 directories * 1 file per directory
        flagMakerTestSetup.withFilesPerDay(1).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // 8 additional = 2 directories * 4 file per directory
        flagMakerTestSetup.withFilesPerDay(4).withNumDays(1).createAdditionalTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        // 10 total files = 2 existing + 8 additional

        Collection<InputFile> files = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(10, files.size());
    }

    @Test
    public void secondCallToNextReturnsEmptyBatch() throws Exception {
        // 2 total files = 2 directories * 1 file per directory
        flagMakerTestSetup.withFilesPerDay(1).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // 8 additional = 2 directories * 4 file per directory
        flagMakerTestSetup.withFilesPerDay(4).withNumDays(1).createAdditionalTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);
        // 10 total files = 2 existing + 8 additional

        simpleFlagDistributor.next(SIZE_VALIDATOR);

        Collection<InputFile> secondBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(0, secondBatch.size());
    }

    @Test
    public void hasNextReturnsTrueInEitherModeWhenFull() throws Exception {
        // 12 files = 2 directories * 6 file per directory
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        assertTrue(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
    }

    @Test
    public void hasNextUpdatesWhenRemainingFilesFallShortOfFullSize() throws Exception {
        // 12 files = 2 directories * 6 file per directory
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // consume 10 of the 12 files
        simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertFalse(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
    }

    @Test
    public void remainingFilesProvidedOnSecondCallToNext() throws Exception {
        // 12 files = 2 directories * 6 file per directory
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // consume 10 of the 12 files
        simpleFlagDistributor.next(SIZE_VALIDATOR);

        // consume remaining 2 files
        Collection<InputFile> secondBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(2, secondBatch.size());
    }

    @Test
    public void hasNextUpdatesWhenRemainingFilesAreDepleted() throws Exception {
        // 12 files = 2 directories * 6 file per directory
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // consume 10 of the 12 files
        simpleFlagDistributor.next(SIZE_VALIDATOR);

        // consume remaining 2 files
        simpleFlagDistributor.next(SIZE_VALIDATOR);

        assertFalse(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));

        Collection<InputFile> thirdBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(0, thirdBatch.size());
    }

    @Test
    public void multipleHasNextReturnSameAnswer() throws Exception {
        // creates 4 files
        flagMakerTestSetup.withFilesPerDay(2).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        assertFalse(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
        assertFalse(simpleFlagDistributor.hasNext(ONLY_FULL_SIZED_FLAGS));
    }

    @Test
    public void hasNextNotRequired() throws Exception {
        // creates 12 files
        flagMakerTestSetup.withFilesPerDay(6).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        Collection<InputFile> files = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(10, files.size());

        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        Collection<InputFile> secondBatch = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(2, secondBatch.size());
    }

    @Test
    public void testLoadRequiredOnceFileListExhausted() throws Exception {
        // 2 total files = 2 directories * 1 file per directory
        flagMakerTestSetup.withFilesPerDay(1).withNumDays(1).createTestFiles();
        simpleFlagDistributor.loadFiles(fooAndBarConfig);

        // consume those 2 files
        assertTrue(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
        Collection<InputFile> files = simpleFlagDistributor.next(SIZE_VALIDATOR);
        assertEquals(2, files.size());

        // 8 additional = 2 directories * 4 file per directory
        flagMakerTestSetup.withFilesPerDay(4).withNumDays(1).createAdditionalTestFiles();
        // do not call loadFiles after creating the additional files
        // confirm that the distributor does not know about the new files
        assertFalse(simpleFlagDistributor.hasNext(DO_NOT_REQUIRE_MAX_SIZE));
    }

    @Test(expected = IllegalStateException.class)
    public void testNextWithoutLoadCausesError() throws Exception {
        // do not create or load files
        FlagFileTestSetup flagMakerTestSetup = new FlagFileTestSetup();
        flagMakerTestSetup.withTestFlagMakerConfig();
        SimpleFlagDistributor simpleFlagDistributor = new SimpleFlagDistributor(flagMakerTestSetup.getFlagMakerConfig());

        // hasNext and next do not cause an error
        simpleFlagDistributor.next(SIZE_VALIDATOR);
    }

    @Test(expected = IllegalStateException.class)
    public void testHasNextWithoutLoadCausesError() throws Exception {
        // do not create or load files
        FlagFileTestSetup flagMakerTestSetup = new FlagFileTestSetup();
        flagMakerTestSetup.withTestFlagMakerConfig();
        SimpleFlagDistributor simpleFlagDistributor = new SimpleFlagDistributor(flagMakerTestSetup.getFlagMakerConfig());

        // hasNext and next do not cause an error
        assertFalse(simpleFlagDistributor.hasNext(false));
    }
}
