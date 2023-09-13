package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

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

    private final Optional<Boolean> useFolderTime;
    private final Optional<Boolean> setFlagFileTimestamp;

    private FlagFileTestSetup flagMakerTestSetup;

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameters(name = "FlagMakerTimestampTest_{index}")
    public static Collection<Object[]> data() {
        // @formatter:off
		return Arrays.asList(new Object[][]{
				{Optional.of(true), Optional.of(true)},
				{Optional.of(true), Optional.of(false)},
				{Optional.of(true), Optional.empty()},
				{Optional.of(false), Optional.of(true)},
				{Optional.of(false), Optional.of(false)},
				{Optional.of(false), Optional.empty()},
				{Optional.empty(), Optional.of(true)},
				{Optional.empty(), Optional.of(false)},
				{Optional.empty(), Optional.empty()}});
		// @formatter:on
    }

    public FlagMakerTimestampTest(Optional<Boolean> useFolderTime, Optional<Boolean> setFlagFileTimestamp) {
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
        File[] flagFiles = runFlagMaker();
        assertEquals("Incorrect number of flagFiles.  Found files: " + FlagFileTestHelper.logFiles(flagFiles), 2, flagFiles.length);
        assertFlagFilesTimestampInExpectedRange(flagFiles, determineExpectedRange());
    }

    private void createInputFiles() throws IOException {
        // two days, 5 files each day, two folders in fmc = 20 flags
        flagMakerTestSetup.withFilesPerDay(5).withNumDays(2).createTestFiles();
    }

    private void configureFlagMakerWithParameters() {
        // true by default - use latest last modified for input files
        this.setFlagFileTimestamp.ifPresent(aBoolean -> flagMakerTestSetup.fmc.setSetFlagFileTimestamp(aBoolean));
        // false by default - use the folder's timestamp regardless of input
        // files
        this.useFolderTime.ifPresent(aBoolean -> flagMakerTestSetup.fmc.setUseFolderTimestamp(aBoolean));
    }

    private File[] runFlagMaker() throws Exception {
        FlagMaker flagMaker = new FlagMaker(flagMakerTestSetup.fmc);
        flagMaker.processFlags();
        return FlagFileTestHelper.listFlagFiles(this.flagMakerTestSetup.fmc);
    }

    private LongRange determineExpectedRange() {
        LongRange range;

        if (useFolderTime.isPresent() && useFolderTime.get()) {
            range = new LongRange(flagMakerTestSetup.getMinFolderTime(), flagMakerTestSetup.getMaxFolderTime());
        } else {
            range = new LongRange(flagMakerTestSetup.getMinLastModified(), flagMakerTestSetup.getMaxLastModified());
        }
        return range;
    }

    private void assertFlagFilesTimestampInExpectedRange(File[] flagFiles, LongRange expectedTimestampRange) {
        for (File file : flagFiles) {
            if (file.getName().endsWith(".flag")) {
                if (!this.setFlagFileTimestamp.isPresent() || this.setFlagFileTimestamp.get()) {
                    assertTrue(expectedTimestampRange.containsLong(file.lastModified()));
                } else {
                    assertFalse(expectedTimestampRange.containsLong(file.lastModified()));
                }
            }
        }
    }
}
