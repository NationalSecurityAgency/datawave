package datawave.util.flag;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Verifies the logic for ensuring only full-sized flags are created, when Flag Maker is configured to do so. There are two configuration mechanisms for
 * preventing partially-sized flag from being created: <br/>
 * - Time (maximum delay, in millis, since the last flag file was created before creating a partially-sized flag file)<br/>
 * - Backlog size (when the number of number of existing flag files exceeds this number, partial files are not created)
 */
@RunWith(Parameterized.class)
public class FlagMakerSizingTest {
    // Backlog threshold values for testing
    private static final int UNSET_BACKLOG_TOLERANCE = Integer.MIN_VALUE;
    private static final int ZERO_BACKLOG_TOLERANCE = 0;
    private static final int HIGH_BACKLOG_TOLERANCE = 10;

    // Time threshold values for testing
    private static final int UNSET_TIMEOUT = Integer.MIN_VALUE;
    private static final int BLOCK_ON_TIME = 100000;
    private static final int NO_TIME_BLOCK = 0;

    // Number of input files in fully-sized flag file
    private static final int FULL_FLAG_SIZE = 10;

    private static final boolean ONLY_FULL_SIZE = true;
    private static final boolean ALLOW_PARTIALS = false;

    private final String testDescription;
    private final boolean expectOnlyFullFlagFiles;
    private final FlagFileTestSetup flagMakerTestSetup;

    private static int instance = 0;

    public FlagMakerSizingTest(int timeout, int backlogThreshold, int fullFlagSize, boolean expectOnlyFullFlagFiles, String description) throws Exception {
        flagMakerTestSetup = new FlagFileTestSetup();
        flagMakerTestSetup.withTestNameForDirectories(this.getClass().getName() + "_" + instance++);
        flagMakerTestSetup.withTestFlagMakerConfig();
        flagMakerTestSetup.withTimeoutMilliSecs(timeout);
        flagMakerTestSetup.withFlagCountThreshold(backlogThreshold);
        flagMakerTestSetup.withMaxFlags(fullFlagSize);
        this.expectOnlyFullFlagFiles = expectOnlyFullFlagFiles;
        this.testDescription = description;
    }

    @After
    public void cleanup() throws IOException {
        flagMakerTestSetup.deleteTestDirectories();
    }

    @Parameterized.Parameters(name = "{4}")
    public static Iterable<Object[]> testCases() {
        // @formatter:off
		return Arrays.asList(new Object[][]{
                    {BLOCK_ON_TIME, ZERO_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ONLY_FULL_SIZE, "block on both time and backlog size"},
                    {BLOCK_ON_TIME, HIGH_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ONLY_FULL_SIZE, "block on time, not backlog size (very high backlog threshold)"},
                    {BLOCK_ON_TIME, UNSET_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ONLY_FULL_SIZE, "block on time while backlog threshold is unset"},
                    {UNSET_TIMEOUT, ZERO_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ONLY_FULL_SIZE, "block on backlog size, not time (unset)"},
                    {UNSET_TIMEOUT, HIGH_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ALLOW_PARTIALS, "no blocking, using high backlog threshold, unset time"},
                    {UNSET_TIMEOUT, UNSET_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ALLOW_PARTIALS, "no blocking, with unset time and unset backlog threshold"},
                    {NO_TIME_BLOCK, ZERO_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ONLY_FULL_SIZE, "block on backlog size, not time"},
                    {NO_TIME_BLOCK, HIGH_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ALLOW_PARTIALS, "no blocking, using high backlog threshold"},
                    {NO_TIME_BLOCK, UNSET_BACKLOG_TOLERANCE, FULL_FLAG_SIZE, ALLOW_PARTIALS, "no blocking, with unset backlog threshold"}});
		// @formatter:on
    }

    /**
     * Asserts when requireFullSizedFlagFiles is true: asserts that only full sized flag files will be created. When false: flag files will be created for which
     * numFiles < maxFlags.
     *
     * @throws Exception
     *             likely due to failure to test files
     */
    @Test
    public void verifyProperFlagFileSizing() throws Exception {
        FlagMaker flagMaker = new FlagMaker(flagMakerTestSetup.getFlagMakerConfig());

        // generate 8 input files, below the "full" threshold
        flagMakerTestSetup.withFilesPerDay(2).withNumDays(2).createTestFiles();
        flagMaker.processFlags();

        if (expectOnlyFullFlagFiles) {
            // wait until full size is reached
            assertNumOfFlagFiles(0);
        } else {
            // create a flag from the partial list
            assertNumOfFlagFiles(1);
            assertEquals(1, countNumFlagsOfSize(8));
        }

        // generate 4 input files, below the "full" threshold
        flagMakerTestSetup.withFilesPerDay(1).withNumDays(2).createAdditionalTestFiles();
        flagMaker.processFlags();

        if (expectOnlyFullFlagFiles) {
            // should create just one full flag file and wait on the remainder
            assertNumOfFlagFiles(1);
            assertEquals(1, countNumFlagsOfSize(FULL_FLAG_SIZE));
        } else {
            // should make another partial flag file
            assertNumOfFlagFiles(2);
            assertEquals(1, countNumFlagsOfSize(4));
        }
    }

    private int countNumFlagsOfSize(int expectedSizeOfFlagFile) {
        int numFoundWithExpectedSize = 0;
        for (File file : FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig())) {
            if (file.getName().endsWith("+" + expectedSizeOfFlagFile + ".flag")) {
                numFoundWithExpectedSize++;
            }
        }
        return numFoundWithExpectedSize;
    }

    private void assertNumOfFlagFiles(int expectedNumber) {
        List<File> files = FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig());
        assertEquals(generateMessage(files), expectedNumber, files.size());
    }

    private String generateMessage(List<File> files) {
        String testCaseDetails = "Expected " + (expectOnlyFullFlagFiles ? "only full" : "partials allowed") + ": " + this.testDescription;
        return "Unexpected number of flag files (" + files.size() + "): " + files + "\n" + testCaseDetails;
    }
}
