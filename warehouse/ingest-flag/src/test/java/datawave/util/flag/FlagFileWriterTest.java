package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

public class FlagFileWriterTest {
    private static final String BASELINE_FLAG_WITH_NO_MARKER = "datawave/util/flag/FlagFileWithoutMarker.flag";
    private static final String BASELINE_FLAG_WITH_MARKER = "datawave/util/flag/FlagFileWithMarker.flag";

    private FlagFileTestSetup flagFileTestSetup;
    private FlagMakerConfig flagMakerConfig;
    private FlagDataTypeConfig dataTypeConfig;
    private Collection<InputFile> inputFiles;
    private FileSystem fs;

    @Rule
    public TestName testName = new TestName();

    private static final String FLAG_MARKER = "XXXX--test-marker--XXXX";

    private String subdirectoryName;

    @Before
    public void before() throws Exception {
        subdirectoryName = this.getClass().getName() + "_" + this.testName.getMethodName();
        flagFileTestSetup = new FlagFileTestSetup().withTestNameForDirectories(subdirectoryName).withTestFlagMakerConfig();
        flagMakerConfig = flagFileTestSetup.getFlagMakerConfig();
        fs = flagFileTestSetup.getFileSystem();
        this.dataTypeConfig = flagFileTestSetup.getInheritedDataTypeConfig();
        this.inputFiles = createInputFiles();
    }

    @After
    public void cleanup() throws IOException {
        flagFileTestSetup.deleteTestDirectories();
    }

    @Test
    public void verifyContentsOfFlagFileWithMarker() throws IOException, URISyntaxException {
        // configure to use marker
        this.dataTypeConfig.setFileListMarker(FLAG_MARKER);

        // write flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        assertFlagFileMatchesBaseline(BASELINE_FLAG_WITH_MARKER);
    }

    @Test
    public void verifyContentsOfFlagFileWithoutMarker() throws IOException, URISyntaxException {
        // by default, marker is not used

        // write flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        assertFlagFileMatchesBaseline(BASELINE_FLAG_WITH_NO_MARKER);
    }

    @Test
    public void doesNotWriteMetricsFileWhenDisabled() throws IOException {
        // disable metrics
        dataTypeConfig.setCollectMetrics("false");

        // write a flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // confirm no metrics files exist
        List<File> metricsFiles = FlagFileTestInspector.retrieveMetricsFiles(flagMakerConfig);
        assertEquals(metricsFiles.toString(), 0, metricsFiles.size());
    }

    @Test
    public void writesMetricsWhenFlagFileCreated() throws IOException {
        dataTypeConfig.setCollectMetrics("true");

        FlagFileWriter flagFileWriter = createFlagFileWriterUsingMetricsWithoutCompression(flagMakerConfig);
        flagFileWriter.writeFlagFile(dataTypeConfig, inputFiles);

        // expect two metrics files
        List<File> metricsFiles = FlagFileTestInspector.retrieveMetricsFiles(flagMakerConfig);
        assertNotNull(metricsFiles);
        assertEquals(metricsFiles.toString(), 2, metricsFiles.size());

        // retrieve metrics file names
        List<String> metricsFileNames = metricsFiles.stream().map(File::getName).collect(Collectors.toList());

        // metrics file name contains flag file name with different extension
        String flagFileName = FlagFileTestInspector.getOnlyFlagFile(this.flagMakerConfig).getName();
        String expectedMetricsFileName = flagFileName.replace(".flag", ".metrics");
        assertTrue(metricsFileNames + " did not contain " + expectedMetricsFileName, metricsFileNames.contains(expectedMetricsFileName));

        // there is also a crc file
        String crcFileName = '.' + expectedMetricsFileName + ".crc";
        assertTrue(metricsFileNames + " did not contain " + crcFileName, metricsFileNames.contains(crcFileName));
    }

    @Test
    public void metricsFileContainsCorrectCounters() throws IOException {
        // will always fail if writesMetricsWhenFlagFileCreated fails
        long startTime = System.currentTimeMillis();

        // create flag file while metrics enabled
        dataTypeConfig.setCollectMetrics("true");
        FlagFileWriter flagFileWriter = createFlagFileWriterUsingMetricsWithoutCompression(flagMakerConfig);
        flagFileWriter.writeFlagFile(dataTypeConfig, inputFiles);

        long stopTime = System.currentTimeMillis();

        // construct expected Path for metrics file
        String flagFileName = FlagFileTestInspector.listFlagFiles(this.flagMakerConfig).get(0).getName();
        String flagFileBaseName = flagFileName.substring(0, flagFileName.lastIndexOf('.'));

        // read metrics
        FlagMetricsFileVerifier flagMetricsFileVerifier = new FlagMetricsFileVerifier(flagFileTestSetup, flagFileBaseName);

        // verify flag file identifier key
        String expectedKeyString = flagFileName.substring(0, flagFileName.lastIndexOf("."));
        assertEquals(expectedKeyString, flagMetricsFileVerifier.getName());

        flagMetricsFileVerifier.assertGroupNames();
        flagMetricsFileVerifier.assertCountersForFilesShowingFlagTimes(startTime, stopTime);
        flagMetricsFileVerifier.assertCountersForInputFileLastModified(flagFileTestSetup.getNamesOfCreatedFiles());
        flagMetricsFileVerifier.assertFlagMakerStartStopTimesInExpectedRange(startTime, stopTime);
    }

    @Test
    public void unusedFlagWriterClosesWithoutError() throws IOException {
        FlagFileWriter flagFileWriter = new FlagFileWriter(flagMakerConfig);
        flagFileWriter.close();
    }

    @Test
    public void flagWriterClosesWithoutErrorAfterWrite() throws IOException {
        FlagFileWriter flagFileWriter = new FlagFileWriter(flagMakerConfig);
        flagFileWriter.writeFlagFile(dataTypeConfig, inputFiles);
        flagFileWriter.close();
    }

    @Test
    public void useTimestampOfMostRecentFile() throws IOException {
        // write a flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // get list of flagged input files to determine the latest last modified time
        List<File> flaggedFiles = FlagFileTestInspector.listFlaggedFiles(flagMakerConfig);
        long flaggedFilesLargestLastModified = flaggedFiles.stream().map(File::lastModified).reduce(Math::max).get();

        // get the last modified time for the flag file
        List<File> flagFiles = FlagFileTestInspector.listFlagFiles(flagMakerConfig);
        long flagFileLastModified = flagFiles.get(0).lastModified();

        // verify flag file last modified equals the latest of the flagged files modification times
        assertEquals(flaggedFilesLargestLastModified, flagFileLastModified);
        assertTrue(0 < flagFileLastModified);
    }

    @Test
    public void renamesFlagFileWhenReady() throws IOException {
        // write a flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // verify file extension
        String flagFileName = FlagFileTestInspector.listFlagFiles(flagMakerConfig).get(0).getName();
        String expectedExtension = ".flag";
        assertTrue("Did not contain proper file extension", flagFileName.endsWith(expectedExtension));
    }

    private void assertFlagFileMatchesBaseline(String testResourceLocation) throws URISyntaxException, IOException {
        // expected flag file
        String expectedFileContents = loadBaselineFileIntoMemory(testResourceLocation);
        expectedFileContents = expectedFileContents.replaceAll("target/test/BulkIngest", "target/" + subdirectoryName + "/test/BulkIngest");

        // load contents of flag file
        File flagFile = FlagFileTestInspector.listFlagFiles(this.flagMakerConfig).get(0);
        final String actualFileContents = new String(Files.readAllBytes(Paths.get(flagFile.getPath())));

        Assert.assertEquals(expectedFileContents, expectedFileContents, actualFileContents);
    }

    private String loadBaselineFileIntoMemory(String testResourceLocation) throws URISyntaxException, IOException {
        URL urlForExpectedContent = this.getClass().getClassLoader().getResource(testResourceLocation);
        assert urlForExpectedContent != null;
        return new String(Files.readAllBytes(Paths.get(urlForExpectedContent.toURI())));
    }

    private TreeSet<InputFile> createInputFiles() throws IOException {
        // creates 2 * filesPerDay number of files: filesPerDay in foo,
        // filesPerDay in bar
        int filesPerDay = 5;
        flagFileTestSetup.withPredicableInputFilenames().withFilesPerDay(filesPerDay).withNumDays(1).createTestFiles();
        TreeSet<InputFile> sortedFiles = new TreeSet<>(InputFile.FIFO);
        sortedFiles.addAll(FlagFileTestInspector.listSortedInputFiles(flagMakerConfig, fs));
        // verify file creation
        assertNotNull(sortedFiles);
        assertEquals(2 * filesPerDay, sortedFiles.size());
        return sortedFiles;
    }

    /**
     * The unit test environment does not have native hadoop libraries to support GzipCodec, so override to not compress
     */
    private FlagFileWriter createFlagFileWriterUsingMetricsWithoutCompression(final FlagMakerConfig flagMakerConfig) throws IOException {
        return new FlagFileWriter(flagMakerConfig) {
            @Override
            FlagMetrics createFlagMetrics(FlagDataTypeConfig fc) {
                return new FlagMetricsWithTestCompatibleCodec(fs, fc.isCollectMetrics());
            }
        };
    }
}
