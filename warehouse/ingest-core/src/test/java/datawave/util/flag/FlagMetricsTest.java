package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

// Also see FlagFileWriterTest.metricsFileContainsCorrectCounters
public class FlagMetricsTest {
    private static LocalFileSystem localFileSystem;
    private FlagFileTestSetup flagFileTestSetup;
    private String metricsDir;

    @Rule
    public TestName testName = new TestName();

    private String flagFileBaseName;

    private FlagMetricsFileVerification flagMetricsFileVerification;
    private long startTime, stopInputFileCreationTime, stopFlaggedTime, stopWritingMetricsTime;

    @BeforeClass
    public static void beforeClass() throws IOException {
        localFileSystem = FileSystem.getLocal(new Configuration());
    }

    @Before
    public void before() throws Exception {
        flagFileTestSetup = new FlagFileTestSetup();
        flagFileTestSetup.withTestFlagMakerConfig();
        flagFileTestSetup.withPredicableInputFilenames();
        flagFileTestSetup.withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());

        this.flagFileBaseName = "target/datawave.util.flag.FlagMetricsTest_" + testName.getMethodName()
                        + "/test/flags/1695222480.00_onehr_foo_c3283902-aa27-44fe-8f66-e246915e4dad+10";

        this.metricsDir = "target/test/metrics" + "/" + testName.getMethodName() + "/metrics/";
        cleanDirectoryContents(this.metricsDir);

        useFlagMetricsAsIntended();

        Path expectedMetricsFilePath = new Path(flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory() + File.separator + flagFileBaseName + ".metrics");
        this.flagMetricsFileVerification = new FlagMetricsFileVerification(expectedMetricsFilePath, flagFileTestSetup);

    }

    private void useFlagMetricsAsIntended() throws IOException {
        // create FlagMetrics and use in expected way
        this.startTime = System.currentTimeMillis();
        FlagMetrics metrics = new FlagMetricsWithTestCompatibleCodec(this.localFileSystem, true);

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
        metrics.writeMetrics(flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory(), flagFileBaseName);
        this.stopWritingMetricsTime = System.currentTimeMillis();
    }

    private void cleanDirectoryContents(String directory) throws IOException {
        // delete the directory and its contents if it already exists
        java.nio.file.Path path = Paths.get(directory);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    @Test
    public void testWriteMetricsCreatesAFile() throws IOException {
        FlagMetrics metrics = new FlagMetricsWithTestCompatibleCodec(this.localFileSystem, true);
        metrics.addFlaggedTime(InputFileSets.SINGLE_FILE.iterator().next());
        metrics.writeMetrics(this.metricsDir, "writeMetricsDoesNotCreateFilesWhenDisabled");

        java.nio.file.Path path = Paths.get(this.metricsDir);
        assertTrue(Files.exists(path));

        List<java.nio.file.Path> foundPaths = Files.list(path).collect(Collectors.toList());
        assertEquals("Expected one metrics file and crc file.  Found: " + foundPaths.toString(), 2, foundPaths.size());
    }

    @Test
    public void writeMetricsDoesNotCreateFilesWhenDisabled() throws IOException {

        FlagMetrics metrics = new FlagMetricsWithTestCompatibleCodec(this.localFileSystem, false);
        metrics.addFlaggedTime(InputFileSets.SINGLE_FILE.iterator().next());
        metrics.writeMetrics(this.metricsDir, "writeMetricsDoesNotCreateFilesWhenDisabled");

        java.nio.file.Path path = Paths.get(this.metricsDir);
        if (Files.exists(path)) {
            fail("Found unexpected files: " + Files.list(path).collect(Collectors.toList()).toString());
        }
    }

    @Test
    public void containsFlagFileName() {
        assertEquals(flagFileBaseName, flagMetricsFileVerification.getName());
    }

    @Test
    public void createsExpectedGroupNames() {
        flagMetricsFileVerification.assertGroupNames();
    }

    @Test
    public void createsCountersWhenFileIsFlagged() {
        flagMetricsFileVerification.assertCountersForFilesShowingFlagTimes(stopInputFileCreationTime, stopFlaggedTime);
    }

    @Test
    public void createsCountersForInputFileLastModified() {
        flagMetricsFileVerification.assertCountersForInputFileLastModified(flagFileTestSetup.getNamesOfCreatedFiles());
    }

    @Test
    public void createsCountersForFlagMakerPhase() {
        flagMetricsFileVerification.assertFlagMakerStartStopTimesInExpectedRange(startTime, stopWritingMetricsTime);
    }

    @Test(expected = IllegalArgumentException.class)
    // If this test starts failing, then get rid of FlagMetricsWithTestCompatibleCodec and use FlagMetrics
    public void gzipCodecFailsInUnitTests() throws IOException {
        FlagMetrics metrics = new FlagMetrics(this.localFileSystem, true);
        metrics.writeMetrics(flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory(), "baseName");
    }
}
