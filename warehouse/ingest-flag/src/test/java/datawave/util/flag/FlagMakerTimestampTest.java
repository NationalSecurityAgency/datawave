package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.math.LongRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests related to the configuration of flag file timestamp, testing all combinations of setSetFlagFileTimestamp and setUseFolderTimestamp
 */
@RunWith(Parameterized.class)
public class FlagMakerTimestampTest {
    private static final Logger log = LoggerFactory.getLogger(FlagMakerTimestampTest.class);
    private static final boolean USE_FOLDER_TIME = true;
    private static final boolean USE_FILE_TIME = false;
    private static final boolean SET_FLAG_TIME_TO_LAST_FILE = true;
    private static final boolean SET_FLAG_TIME_TO_CREATED_TIME = false;
    private static final Boolean UNSET = null;
    private static final int EXPECTED_NUM_FLAG_FILES = 2;

    private final Boolean useFolderTime;
    private final Boolean setFlagFileTimestamp;

    private FlagFileTestSetup flagMakerTestSetup;

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameters(name = "FlagMakerTimestampTest_{index}")
    public static Collection<Object[]> data() {
        // @formatter:off
		return Arrays.asList(new Object[][]{
				{USE_FOLDER_TIME, SET_FLAG_TIME_TO_LAST_FILE},
				{USE_FOLDER_TIME, SET_FLAG_TIME_TO_CREATED_TIME},
				{USE_FOLDER_TIME, UNSET},
				{USE_FILE_TIME, SET_FLAG_TIME_TO_LAST_FILE},
				{USE_FILE_TIME, SET_FLAG_TIME_TO_CREATED_TIME},
				{USE_FILE_TIME, UNSET},
				{UNSET, SET_FLAG_TIME_TO_LAST_FILE},
				{UNSET, SET_FLAG_TIME_TO_CREATED_TIME},
				{UNSET, UNSET}});
		// @formatter:on
    }

    public FlagMakerTimestampTest(Boolean useFolderTime, Boolean setFlagFileTimestamp) {
        this.useFolderTime = useFolderTime;
        this.setFlagFileTimestamp = setFlagFileTimestamp;
    }

    @Before
    public void setUp() throws Exception {
        log.error("testName: " + testName.getMethodName());

        flagMakerTestSetup = new FlagFileTestSetup();
        flagMakerTestSetup.withTestNameForDirectories(testName.getMethodName().replaceAll("]", "-").replaceAll("\\[", "-"));
        flagMakerTestSetup.withTestFlagMakerConfig();
    }

    @After
    public void cleanup() throws IOException {
        flagMakerTestSetup.deleteTestDirectories();
    }

    @Test
    public void verifyFlagFileTimestamp() throws Exception {
        createInputFiles();
        configureFlagMakerWithParameters();
        List<File> flagFiles = runFlagMaker();
        assertEquals("Incorrect number of flagFiles.  Found files: " + FlagFileTestInspector.logFiles(flagFiles), EXPECTED_NUM_FLAG_FILES, flagFiles.size());
        assertFlagFilesTimestampInExpectedRange(flagFiles, determineExpectedRange());
    }

    private void createInputFiles() throws IOException {
        // Two days, five files each day, two folders in getFlagMakerConfig() = twenty input files
        flagMakerTestSetup.withFilesPerDay(5).withNumDays(2).createTestFiles();
    }

    private void configureFlagMakerWithParameters() {
        // true by default - use latest last modified of input files
        if (null != this.setFlagFileTimestamp) {
            flagMakerTestSetup.getFlagMakerConfig().setSetFlagFileTimestamp(this.setFlagFileTimestamp);
        }
        // true: use the folder's timestamp regardless of input files
        // false (default): use time of input file
        if (null != this.useFolderTime) {
            flagMakerTestSetup.getFlagMakerConfig().setUseFolderTimestamp(this.useFolderTime);
        }
    }

    private List<File> runFlagMaker() throws Exception {
        FlagMaker flagMaker = new FlagMaker(this.flagMakerTestSetup.getFlagMakerConfig());
        flagMaker.processFlags();
        return FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig());
    }

    private LongRange determineExpectedRange() {
        LongRange range;

        if (null != useFolderTime && useFolderTime == USE_FOLDER_TIME) {
            range = new LongRange(flagMakerTestSetup.getMinFolderTime(), flagMakerTestSetup.getMaxFolderTime());
        } else {
            range = new LongRange(flagMakerTestSetup.getMinLastModified(), flagMakerTestSetup.getMaxLastModified());
        }
        return range;
    }

    private void assertFlagFilesTimestampInExpectedRange(List<File> flagFiles, LongRange expectedTimestampRange) {
        for (File file : flagFiles) {
            if (file.getName().endsWith(".flag")) {
                if (this.setFlagFileTimestamp == null || SET_FLAG_TIME_TO_LAST_FILE == this.setFlagFileTimestamp) {
                    assertTrue(expectedTimestampRange.containsLong(file.lastModified()));
                } else {
                    assertFalse(expectedTimestampRange.containsLong(file.lastModified()));
                }
            }
        }
    }
}
