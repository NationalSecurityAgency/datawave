package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test FlagMetrics Also see FlagFileWriterTest.metricsFileContainsCorrectCounters that verifies the FlagFileWriter creates a valid flag metrics file as well
 */
public class FlagMetricsTest {
    private static final String FLAG_FILE_BASE_NAME = "1695222480.00_onehr_foo_c3283902-aa27-44fe-8f66-e246915e4dad+10";

    private static LocalFileSystem LOCAL_FILE_SYSTEM;

    private FlagFileTestSetup flagFileTestSetup;
    private String metricsDirectory;

    private long startTime, stopInputFileCreationTime, stopFlaggedTime, stopWritingMetricsTime;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void beforeClass() throws IOException {
        LOCAL_FILE_SYSTEM = FileSystem.getLocal(new Configuration());
    }

    @Before
    public void before() throws Exception {
        this.flagFileTestSetup = new FlagFileTestSetup();
        this.flagFileTestSetup.withTestFlagMakerConfig();
        this.flagFileTestSetup.withPredicableInputFilenames();

        // puts method name under /target, e.g. target/datawave.util.flag.FlagMetricsTest_gzipCodecFailsInUnitTests/flagMetrics
        String testMethodName = this.testName.getMethodName();
        String directoryNameForTest = this.getClass().getName() + "_" + testMethodName;
        this.flagFileTestSetup.withTestNameForDirectories(directoryNameForTest);

        this.metricsDirectory = flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory();
        cleanDirectoryContents(this.metricsDirectory);
        assertNumberOfFilesInMetricsDirectory(0);
    }

    @Test
    public void testWriteMetricsCreatesAFile() throws IOException {
        assertNumberOfFilesInMetricsDirectory(0);

        FlagMetrics metrics = new FlagMetricsWithTestCompatibleCodec(LOCAL_FILE_SYSTEM, true);
        metrics.addFlaggedTime(InputFileSets.SINGLE_FILE.iterator().next());
        metrics.writeMetrics(this.metricsDirectory, FLAG_FILE_BASE_NAME);

        assertNumberOfFilesInMetricsDirectory(2);
    }

    @Test
    public void writeMetricsDoesNotCreateFilesWhenDisabled() throws IOException {
        assertNumberOfFilesInMetricsDirectory(0);

        FlagMetrics metrics = new FlagMetricsWithTestCompatibleCodec(LOCAL_FILE_SYSTEM, false);
        metrics.addFlaggedTime(InputFileSets.SINGLE_FILE.iterator().next());
        metrics.writeMetrics(this.metricsDirectory, FLAG_FILE_BASE_NAME);

        assertNumberOfFilesInMetricsDirectory(0);
    }

    @Test
    public void containsFlagFileName() throws IOException {
        useFlagMetricsAsIntended();

        FlagMetricsFileVerifier flagMetricsFileVerifier = new FlagMetricsFileVerifier(this.flagFileTestSetup, FLAG_FILE_BASE_NAME);
        assertEquals(FLAG_FILE_BASE_NAME, flagMetricsFileVerifier.getName());
    }

    @Test
    public void createsExpectedGroupNames() throws IOException {
        useFlagMetricsAsIntended();

        FlagMetricsFileVerifier flagMetricsFileVerifier = new FlagMetricsFileVerifier(this.flagFileTestSetup, FLAG_FILE_BASE_NAME);
        flagMetricsFileVerifier.assertGroupNames();
    }

    @Test
    public void createsCountersWhenFileIsFlagged() throws IOException {
        useFlagMetricsAsIntended();

        FlagMetricsFileVerifier flagMetricsFileVerifier = new FlagMetricsFileVerifier(this.flagFileTestSetup, FLAG_FILE_BASE_NAME);
        flagMetricsFileVerifier.assertCountersForFilesShowingFlagTimes(stopInputFileCreationTime, stopFlaggedTime);
    }

    @Test
    public void createsCountersForInputFileLastModified() throws IOException {
        useFlagMetricsAsIntended();

        FlagMetricsFileVerifier flagMetricsFileVerifier = new FlagMetricsFileVerifier(this.flagFileTestSetup, FLAG_FILE_BASE_NAME);
        flagMetricsFileVerifier.assertCountersForInputFileLastModified(flagFileTestSetup.getNamesOfCreatedFiles());
    }

    @Test
    public void createsCountersForFlagMakerPhase() throws IOException {
        useFlagMetricsAsIntended();

        FlagMetricsFileVerifier flagMetricsFileVerifier = new FlagMetricsFileVerifier(this.flagFileTestSetup, FLAG_FILE_BASE_NAME);
        flagMetricsFileVerifier.assertFlagMakerStartStopTimesInExpectedRange(startTime, stopWritingMetricsTime);
    }

    @Test(expected = IllegalArgumentException.class)
    // If this test starts failing, then get rid of FlagMetricsWithTestCompatibleCodec and use FlagMetrics for all the tests
    public void gzipCodecFailsInUnitTests() throws IOException {
        FlagMetrics metrics = new FlagMetrics(LOCAL_FILE_SYSTEM, true);
        metrics.writeMetrics(flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory(), FLAG_FILE_BASE_NAME);
    }

    private void cleanDirectoryContents(String directory) throws IOException {
        // delete the directory and its contents if it already exists
        java.nio.file.Path path = Paths.get(directory);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    private void useFlagMetricsAsIntended() throws IOException {
        // create FlagMetrics and use in expected way
        this.startTime = System.currentTimeMillis();
        FlagMetrics metrics = new FlagMetricsWithTestCompatibleCodec(LOCAL_FILE_SYSTEM, true);

        // create input files
        flagFileTestSetup.createTestFiles();
        this.stopInputFileCreationTime = System.currentTimeMillis();

        // register each file with FlagMetrics, then simulate its flagging and register flagged action as well
        for (InputFile inputFile : FlagFileTestInspector.listSortedInputFiles(flagFileTestSetup.getFlagMakerConfig(), flagFileTestSetup.getFileSystem())) {
            metrics.addInputFileTimestamp(inputFile);
            inputFile.setMoved(true);
            inputFile.updateCurrentDir(InputFile.TrackedDir.FLAGGED_DIR);
            metrics.addFlaggedTime(inputFile);
        }
        this.stopFlaggedTime = System.currentTimeMillis();

        // write metrics to directory
        metrics.writeMetrics(flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory(), FLAG_FILE_BASE_NAME);
        this.stopWritingMetricsTime = System.currentTimeMillis();
    }

    private void assertNumberOfFilesInMetricsDirectory(int expectedNumber) throws IOException {
        java.nio.file.Path path = Paths.get(this.metricsDirectory);
        if (Files.exists(path)) {
            List<File> foundFiles = Files.list(path).map(java.nio.file.Path::toFile).filter(File::isFile).collect(Collectors.toList());
            assertEquals("Expected " + expectedNumber + " files in " + this.metricsDirectory + ".  Found: " + foundFiles.size() + ": " + foundFiles.toString(),
                            expectedNumber, foundFiles.size());
        } else if (expectedNumber > 0) {
            fail("Directory " + this.metricsDirectory + " doesn't exist.  Expected " + expectedNumber + " files.");
        }
    }
}
