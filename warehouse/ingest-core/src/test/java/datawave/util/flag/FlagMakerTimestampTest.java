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

    private final Boolean useFolderTime;
    private final Boolean setFlagFileTimestamp;

    private FlagFileTestSetup flagMakerTestSetup;

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameters(name = "FlagMakerTimestampTest_{index}")
    public static Collection<Object[]> data() {
        // @formatter:off
		return Arrays.asList(new Object[][]{
				{true, true},
				{true, false},
				{true, null},
				{false, true},
				{false, false},
				{false, null},
				{null, true},
				{null, false},
				{null, null}});
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
        assertEquals("Incorrect number of flagFiles.  Found files: " + FlagFileTestInspector.logFiles(flagFiles), 2, flagFiles.size());
        assertFlagFilesTimestampInExpectedRange(flagFiles, determineExpectedRange());
    }

    private void createInputFiles() throws IOException {
        // two days, 5 files each day, two folders in getFlagMakerConfig() = 20 flags
        flagMakerTestSetup.withFilesPerDay(5).withNumDays(2).createTestFiles();
    }

    private void configureFlagMakerWithParameters() {
        // true by default - use latest last modified for input files
        if (null != this.setFlagFileTimestamp) {
            flagMakerTestSetup.getFlagMakerConfig().setSetFlagFileTimestamp(this.setFlagFileTimestamp);
        }
        // false by default - use the folder's timestamp regardless of input
        // files
        if (null != this.useFolderTime) {
            flagMakerTestSetup.getFlagMakerConfig().setUseFolderTimestamp(this.useFolderTime);
        }
    }

    private List<File> runFlagMaker() throws Exception {
        FlagMaker flagMaker = new FlagMaker(flagMakerTestSetup.getFlagMakerConfig());
        flagMaker.processFlags();
        return FlagFileTestInspector.listFlagFiles(this.flagMakerTestSetup.getFlagMakerConfig());
    }

    private LongRange determineExpectedRange() {
        LongRange range;

        if (null != useFolderTime && useFolderTime) {
            range = new LongRange(flagMakerTestSetup.getMinFolderTime(), flagMakerTestSetup.getMaxFolderTime());
        } else {
            range = new LongRange(flagMakerTestSetup.getMinLastModified(), flagMakerTestSetup.getMaxLastModified());
        }
        return range;
    }

    private void assertFlagFilesTimestampInExpectedRange(List<File> flagFiles, LongRange expectedTimestampRange) {
        for (File file : flagFiles) {
            if (file.getName().endsWith(".flag")) {
                if (this.setFlagFileTimestamp == null || this.setFlagFileTimestamp) {
                    assertTrue(expectedTimestampRange.containsLong(file.lastModified()));
                } else {
                    assertFalse(expectedTimestampRange.containsLong(file.lastModified()));
                }
            }
        }
    }
}
