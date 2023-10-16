package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.util.flag.config.FlagMakerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * See related test classes: FlagMakerSizingTest, verifies logic related to sizing of flag files based on various configurations FlagMakerTimestampTest,
 * verifies logic related to the timestamp used for flag files based on configuration
 */
public class FlagMakerTest {
    // trailing slash added by validate()
    public static final String CONFIG_BASE_HDFS_DIR = "target/test/BulkIngest/";

    private static final Logger LOG = LoggerFactory.getLogger(FlagMakerTest.class);
    private static final int COUNTER_LIMIT = 102;
    private static final int NUM_INPUT_FILES = 80;

    private FlagFileTestSetup flagMakerTestSetup;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {
        LOG.error("testName: " + testName.getMethodName());

        flagMakerTestSetup = new FlagFileTestSetup();
        flagMakerTestSetup.withTestNameForDirectories("FlagMakerTest_" + testName.getMethodName());
        flagMakerTestSetup.withTestFlagMakerConfig();
        // create 20 files per day (2) per directory (2) = 20 * 2 * 2 = 80
        flagMakerTestSetup.withFilesPerDay(20).withNumDays(2).createTestFiles();

        FlagDataTypeConfig flagDataTypeConfig = flagMakerTestSetup.getFlagMakerConfig().getFlagConfigs().get(0);
        // by setting the timeout to -1, the FlagMaker will use whatever files
        // it has without waiting to create a full sized file
        flagDataTypeConfig.setTimeoutMilliSecs(-1); // timeout will have always occurred

        // verify test assumptions
        for (FlagDataTypeConfig flagConfig : flagMakerTestSetup.getFlagMakerConfig().getFlagConfigs()) {
            assertEquals(10, flagConfig.getMaxFlags());
        }
        int numberOfGeneratedInputFiles = FlagFileTestInspector.listFilesInInputDirectory(flagMakerTestSetup.getFlagMakerConfig()).size();
        assertEquals("Test setup failed to create necessary input files (across directories foo and bar)", NUM_INPUT_FILES, numberOfGeneratedInputFiles);
    }

    @After
    public void cleanup() throws IOException {
        flagMakerTestSetup.deleteTestDirectories();
    }

    @Test
    public void testProcessFlags() throws Exception {
        // 80 input files / 10 input files per flag = 8 flags
        int expectedNumberOfFlagFiles = 8;

        FlagMaker flagMaker = new FlagMaker(this.flagMakerTestSetup.getFlagMakerConfig());
        flagMaker.processFlags();

        List<File> flagFiles = FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig());
        assertEquals("Unexpected number of flag files: " + FlagFileTestInspector.logFiles(flagFiles), expectedNumberOfFlagFiles, flagFiles.size());

        for (File file : flagFiles) {
            String fileName = file.getName();
            assertTrue("Unexpected file ending for " + fileName, fileName.endsWith(".flag"));
        }
    }

    @Test
    public void testCounterLimitExceeded() throws Exception {
        int expectedMax = (COUNTER_LIMIT - 2) / 2;

        // set the max flags to something larger than the expected limit based
        // on the counters
        // two days, expectedMax + 20 files each day
        flagMakerTestSetup.withTestFlagMakerConfig();
        flagMakerTestSetup.withMaxFlags(expectedMax + 15);
        flagMakerTestSetup.withFilesPerDay(expectedMax + 20).withNumDays(2).createTestFiles();
        int totalCreatedFiles = 2 * 2 * (expectedMax + 20);

        // Configure FlagMaker to look for created files
        FlagMaker flagMaker = new FlagMaker(flagMakerTestSetup.getFlagMakerConfig());
        flagMaker.config.set("mapreduce.job.counters.max", Integer.toString(COUNTER_LIMIT));
        flagMaker.processFlags();

        // Inspect resultant flag files, counter the number sized to exactly "expectedMax"
        List<File> files = FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig());
        assertEquals("Unexpected number of files: " + FlagFileTestInspector.logFiles(files), (totalCreatedFiles / expectedMax), files.size());
        for (File file : files) {
            String fileName = file.getName();
            assertTrue("Unexpected number of input files in flag file name: " + fileName, fileName.endsWith("+" + expectedMax + ".flag"));
        }
    }

    private void createFlagsWithSizeConstraint(int desiredInputFilesPerFlag) throws IOException {
        int expectedFlagFileLength = getExpectedFlagFileLength(desiredInputFilesPerFlag);

        // Set the maximum flag file length to hold no more than the expected
        // size of a flag file containing 5 input files
        FlagMakerConfig flagMakerConfig = flagMakerTestSetup.getFlagMakerConfig();

        // one character larger than expected file length
        flagMakerConfig.setMaxFileLength(expectedFlagFileLength + 1);

        // ensure max flags is not a limiting factor by making it slightly larger than the desired number
        FlagDataTypeConfig flagDataTypeConfig = flagMakerConfig.getFlagConfigs().get(0);
        flagDataTypeConfig.setMaxFlags(desiredInputFilesPerFlag + 1);

        FlagMaker flagMaker = new FlagMaker(flagMakerConfig);

        // set counter limit very high to eliminate this constraint
        flagMaker.config.set("mapreduce.job.counters.max", Integer.toString(Integer.MAX_VALUE));

        flagMaker.processFlags();
    }

    @Test
    public void verifyForVerySmallThreshold() throws Exception {
        int desiredInputFilesPerFlag = 1;

        createFlagsWithSizeConstraint(desiredInputFilesPerFlag);

        verifyNumberOfFlagFiles(desiredInputFilesPerFlag);
        verifyFlagFileLengths(desiredInputFilesPerFlag);
    }

    @Test
    public void verifyForModerateThreshold() throws Exception {
        int desiredInputFilesPerFlag = 5;

        createFlagsWithSizeConstraint(desiredInputFilesPerFlag);

        verifyNumberOfFlagFiles(desiredInputFilesPerFlag);
        verifyFlagFileLengths(desiredInputFilesPerFlag);
    }

    @Test
    public void verifyForNonDivisibleNumber() throws Exception {
        // 80 / 6 will result in a remainder of two input files
        int desiredInputFilesPerFlag = 6;

        createFlagsWithSizeConstraint(desiredInputFilesPerFlag);

        verifyNumberOfFlagFiles(desiredInputFilesPerFlag);
        verifyFlagFileLengths(desiredInputFilesPerFlag);
    }

    @Test
    public void verifyForLargeThreshold() throws Exception {
        int desiredInputFilesPerFlag = 80;

        createFlagsWithSizeConstraint(desiredInputFilesPerFlag);

        verifyNumberOfFlagFiles(desiredInputFilesPerFlag);
        verifyFlagFileLengths(desiredInputFilesPerFlag);
    }

    private void verifyNumberOfFlagFiles(int desiredInputFilesPerFlag) {
        int expectedNumberOfFullFlags = (NUM_INPUT_FILES / desiredInputFilesPerFlag);
        int sizeOfRemainder = NUM_INPUT_FILES - (expectedNumberOfFullFlags * desiredInputFilesPerFlag);

        Map<Integer,Integer> flagFileSizeToNumberFound = countFlagFilesBySize();
        Map<Integer,Integer> expectedCounts = new HashMap<>();
        expectedCounts.put(desiredInputFilesPerFlag, expectedNumberOfFullFlags);
        if (sizeOfRemainder > 0) {
            expectedCounts.put(sizeOfRemainder, 1); // expect one flag file for
                                                    // the remaining input files
        }
        assertEquals(getDebuggingMessage(flagFileSizeToNumberFound), expectedCounts, flagFileSizeToNumberFound);
    }

    private String getDebuggingMessage(Map<Integer,Integer> flagFileSizeToNumberFound) {
        return "{flag file size = number found} " + flagFileSizeToNumberFound.toString() + "\n"
                        + FlagFileTestInspector.logFiles(FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig()));
    }

    /**
     * Retrieves the list of flag files created for this test. Produces a count of flag files by size. The map's key is the number of input files per flag (flag
     * file size). The map's value is the number of flag files of that size.
     */
    private Map<Integer,Integer> countFlagFilesBySize() {
        Map<Integer,Integer> flagFileSizeToNumberFound = new HashMap<>();

        List<File> flagFiles = FlagFileTestInspector.listFlagFiles(flagMakerTestSetup.getFlagMakerConfig());
        assertTrue("Expected one or more flag files", flagFiles.size() > 0);

        for (File flagFile : flagFiles) {
            String flagFileName = flagFile.getName();

            // extract number of input files from flag file name
            int indexOfLastPlus = flagFileName.lastIndexOf("+");
            int indexOfDotAfterPlus = flagFileName.indexOf('.', indexOfLastPlus);
            int numberOfInputFiles = Integer.parseInt(flagFileName.substring(indexOfLastPlus + 1, indexOfDotAfterPlus));

            // increment counts
            flagFileSizeToNumberFound.putIfAbsent(numberOfInputFiles, 0);
            flagFileSizeToNumberFound.put(numberOfInputFiles, (flagFileSizeToNumberFound.get(numberOfInputFiles) + 1));
        }

        return flagFileSizeToNumberFound;
    }

    private void verifyFlagFileLengths(int desiredInputFilesPerFlag) throws IOException {
        int expectedFlagFileLength = getExpectedFlagFileLength(desiredInputFilesPerFlag);

        List<File> flagFiles = FlagFileTestInspector.listFlagFiles(flagMakerTestSetup.getFlagMakerConfig());
        assertTrue("Expected one or more flag files", flagFiles.size() > 0);

        for (File flagFile : flagFiles) {
            int flagFileLength = com.google.common.io.Files.asCharSource(flagFile, Charset.defaultCharset()).read().length();
            // ignore the last file because it may be a partial file with the remaining input files
            if (flagFile != flagFiles.get(flagFiles.size() - 1)) {
                Assert.assertEquals(FlagFileTestInspector.logFileContents(flagFile), expectedFlagFileLength, flagFileLength);
            }
        }
    }

    private int getExpectedFlagFileLength(int numInputFilesPerFlag) {
        String expectedInputFileLength = this.flagMakerTestSetup.getFlagMakerConfig().getBaseHDFSDir()
                        + "flagged/bar/2013/01/01/5a2078da-1569-4cb9-bd50-f95fc53934e7";

        // note flag file length is a function of the number of input files per flag

        // datawave home + script
        int expectedFlagFileLength = "target/test/bin/ingest/bulk-ingest.sh".length();
        // script arguments
        expectedFlagFileLength += " 10 -inputFormat datawave.ingest.input.reader.event.EventSequenceFileInputFormat".length();
        // space or comma for each file
        expectedFlagFileLength += numInputFilesPerFlag;
        // names of the files
        expectedFlagFileLength += (expectedInputFileLength.length() * numInputFilesPerFlag);
        // space at the end of the arguments
        expectedFlagFileLength += 1;
        // new line
        expectedFlagFileLength += 1;
        return expectedFlagFileLength;
    }
}
