package datawave.util.flag;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FlagFileWriterTest {
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
        flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig().withTestNameForDirectories(subdirectoryName);
        flagMakerConfig = flagFileTestSetup.fmc;
        fs = flagFileTestSetup.fs;
        this.dataTypeConfig = flagFileTestSetup.getInheritedDataTypeConfig();
        this.inputFiles = createInputFiles(5);
    }

    @After
    public void cleanup() throws IOException {
        flagFileTestSetup.deleteTestDirectories();
    }

    @Test
    public void verifyContentsOfFlagFileWithMarker() throws IOException, URISyntaxException {
        // configure to use marker
        FlagDataTypeConfig fc = flagMakerConfig.getFlagConfigs().get(0); // get
                                                                         // config
                                                                         // for
                                                                         // "foo"
                                                                         // flag
                                                                         // datatype
        fc.setFileListMarker(FLAG_MARKER);

        // write flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // baseline file with marker
        assertFlagFileMatchesBaseline("datawave/util/flag/FlagFileWithMarker.flag");
    }

    @Test
    public void verifyContentsOfFlagFileWithoutMarker() throws IOException, URISyntaxException {
        // by default, marker is not used

        // write flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // baseline file with NO marker
        assertFlagFileMatchesBaseline("datawave/util/flag/FlagFileWithoutMarker.flag");
    }

    @Test
    public void doesNotWriteMetricsFileWhenDisabled() throws IOException {
        dataTypeConfig.setCollectMetrics("false");

        // write a flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // confirm no metrics files exist
        File[] metricsFiles = FlagFileTestHelper.retrieveMetricsFiles(flagMakerConfig);
        assertEquals(Arrays.toString(metricsFiles), 0, metricsFiles.length);
    }

    @Test
    public void writesMetricsWhenFlagFileCreated() throws IOException {
        dataTypeConfig.setCollectMetrics("true");

        FlagFileWriter flagFileWriter = createFlagFileWriterUsingMetricsWithoutCompression(flagMakerConfig);
        flagFileWriter.writeFlagFile(dataTypeConfig, inputFiles);

        // expect two metrics files
        File[] metricsFiles = FlagFileTestHelper.retrieveMetricsFiles(flagMakerConfig);
        assertNotNull(metricsFiles);
        assertEquals(Arrays.toString(metricsFiles), 2, metricsFiles.length);

        // check metrics file names
        List<String> metricsFileNames = Arrays.stream(metricsFiles).map(File::getName).collect(Collectors.toList());

        // contains metrics file
        String flagFileName = FlagFileTestHelper.listFlagFiles(this.flagMakerConfig)[0].getName();
        String expectedMetricsFileName = flagFileName.replace(".flag", ".metrics");
        assertTrue(metricsFileNames + " did not contain " + expectedMetricsFileName, metricsFileNames.contains(expectedMetricsFileName));

        // contains crc file
        String crcFileName = '.' + expectedMetricsFileName + ".crc";
        assertTrue(metricsFileNames + " did not contain " + crcFileName, metricsFileNames.contains(crcFileName));
    }

    @Test
    // will fail when writesMetricsWhenFlagFileCreated fails
    public void metricsFileContainsCorrectCounters() throws IOException {
        long startTime = System.currentTimeMillis();

        // create flag file while metrics enabled
        dataTypeConfig.setCollectMetrics("true");
        FlagFileWriter flagFileWriter = createFlagFileWriterUsingMetricsWithoutCompression(flagMakerConfig);
        flagFileWriter.writeFlagFile(dataTypeConfig, inputFiles);

        long stopTime = System.currentTimeMillis();

        // construct expected Path for metrics file
        String flagFileName = FlagFileTestHelper.listFlagFiles(this.flagMakerConfig)[0].getName();
        String expectedMetricsFileName = flagFileName.replace(".flag", ".metrics");
        Path expectedMetricsFilePath = new Path(flagMakerConfig.getFlagMetricsDirectory(), expectedMetricsFileName);

        // read metrics
        SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(expectedMetricsFilePath));
        Text key = new Text();
        Counters val = new Counters();
        reader.next(key, val);

        // verify flag file identifier key
        String expectedKeyString = flagFileName.substring(0, flagFileName.lastIndexOf("."));
        assertEquals(expectedKeyString, key.toString());

        // verify expected groups
        List<String> expectedGroupNames = Arrays.asList("FlagFile", "InputFile", "datawave.metrics.util.flag.InputFile");
        List<String> actual = new ArrayList<>();
        val.getGroupNames().forEach(actual::add);
        assertTrue(actual.containsAll(expectedGroupNames));

        // Counter Group "FlagFile" contains input files names and current
        // system timestamps for each
        assertCountersForFlagFileTimes(startTime, stopTime, val.getGroup("FlagFile"));

        // Counter Group "InputFile" contains input files names and lastModified
        // timestamps for each
        assertCountersForInputFileLastModified(flagFileTestSetup.getNamesOfCreatedFiles(), val.getGroup("InputFile"));

        // Counter Group "datawave.metrics.util.flag.InputFile" contains flag
        // maker start and stop times
        assertFlagMakerTimesInExpectedRange(startTime, stopTime, val.getGroup("datawave.metrics.util.flag.InputFile"));
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

        // get list of flagged input files to determine the latest last modified
        // time
        List<File> flaggedFiles = FlagFileTestHelper.retrieveFlaggedFiles(flagMakerConfig);
        long flaggedFilesLargestLastModified = flaggedFiles.stream().map(File::lastModified).reduce(Math::max).get();

        // get the last modified time for the flag file
        File[] flagFiles = FlagFileTestHelper.listFlagFiles(flagMakerConfig);
        long flagFileLastModified = flagFiles[0].lastModified();

        // verity expectations
        assertEquals(flaggedFilesLargestLastModified, flagFileLastModified);
        assertTrue(0 < flagFileLastModified);
    }

    @Test
    public void renamesFlagFileWhenReady() throws IOException {
        // write a flag file
        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // verify file extension
        String flagFileName = FlagFileTestHelper.listFlagFiles(flagMakerConfig)[0].getName();
        String expectedExtension = ".flag";
        assertTrue("Did not contain proper file extension", flagFileName.endsWith(expectedExtension));
    }

    private void assertFlagFileMatchesBaseline(String testResourceLocation) throws URISyntaxException, IOException {
        // expected flag file
        String expectedFileContents = loadBaselineFileIntoMemory(testResourceLocation);
        expectedFileContents = expectedFileContents.replaceAll("target/test/BulkIngest", "target/" + subdirectoryName + "/test/BulkIngest");

        // load contents of flag file
        File flagFile = FlagFileTestHelper.listFlagFiles(this.flagMakerConfig)[0];
        final String actualFileContents = new String(Files.readAllBytes(Paths.get(flagFile.getPath())));

        // compare
        Assert.assertEquals(expectedFileContents, expectedFileContents, actualFileContents);
    }

    private String loadBaselineFileIntoMemory(String testResourceLocation) throws URISyntaxException, IOException {
        URL urlForExpectedContent = this.getClass().getClassLoader().getResource(testResourceLocation);
        return new String(Files.readAllBytes(Paths.get(urlForExpectedContent.toURI())));
    }

    private TreeSet<InputFile> createInputFiles(int filesPerDay) throws IOException {
        // creates 2 * filesPerDay number of files: filesPerDay in foo,
        // filesPerDay in bar
        flagFileTestSetup.withPredicableInputFilenames().withFilesPerDay(filesPerDay).withNumDays(1).createTestFiles();
        TreeSet<InputFile> sortedFiles = new TreeSet<>(InputFile.FIFO);
        sortedFiles.addAll(FlagFileTestHelper.listSortedInputFiles(flagMakerConfig, fs));
        // verify file creation
        assertNotNull(sortedFiles);
        assertEquals(2 * filesPerDay, sortedFiles.size());
        return sortedFiles;
    }

    private void assertCountersForFlagFileTimes(long startTime, long stopTime, CounterGroup group) {
        List<String> actualFileNames = StreamSupport.stream(group.spliterator(), false).map(Counter::getName).collect(Collectors.toList());
        Collection<String> expectedFileNames = flagFileTestSetup.getNamesOfCreatedFiles();
        assertTrue(expectedFileNames.containsAll(actualFileNames));
        assertTrue(actualFileNames.containsAll(expectedFileNames));

        List<Long> actualCurrentTimes = StreamSupport.stream(group.spliterator(), false).map(Counter::getValue).collect(Collectors.toList());
        for (Long actualTime : actualCurrentTimes) {
            assertTrue(startTime < actualTime);
            assertTrue(stopTime > actualTime);
        }
    }

    private void assertCountersForInputFileLastModified(Collection<String> expectedFileNames, CounterGroup group) {
        List<String> actualFileNames;
        List<Long> actualCurrentTimes;
        actualFileNames = StreamSupport.stream(group.spliterator(), false).map(Counter::getName).collect(Collectors.toList());
        assertTrue(expectedFileNames.containsAll(actualFileNames));
        assertTrue(actualFileNames.containsAll(expectedFileNames));

        actualCurrentTimes = StreamSupport.stream(group.spliterator(), false).map(Counter::getValue).collect(Collectors.toList());
        Collection<Long> expectedTimes = flagFileTestSetup.lastModifiedTimes;
        assertTrue(expectedTimes.containsAll(actualCurrentTimes));
        assertTrue(actualCurrentTimes.containsAll(expectedTimes));
    }

    private void assertFlagMakerTimesInExpectedRange(long testSubjectExecutionStartTime, long testSubjectExecutionStopTime, CounterGroup group) {
        long counterStartTime = group.findCounter("FLAGMAKER_START_TIME").getValue();
        long counterStopTime = group.findCounter("FLAGMAKER_END_TIME").getValue();
        assertTrue(testSubjectExecutionStartTime < counterStartTime);
        assertTrue(counterStartTime < counterStopTime);
        assertTrue(testSubjectExecutionStopTime > counterStopTime);
    }

    /**
     * The unit test environment does not have native hadoop libraries to support GzipCodec, so override to not compress
     */
    private FlagFileWriter createFlagFileWriterUsingMetricsWithoutCompression(final FlagMakerConfig flagMakerConfig) throws IOException {
        return new FlagFileWriter(flagMakerConfig) {
            @Override
            FlagMetrics createFlagMetrics(FileSystem fs, FlagDataTypeConfig fc) {
                return new FlagMetrics(fs, fc.isCollectMetrics()) {
                    @Override
                    SequenceFile.Writer.Option getCompressionOption() {
                        return SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE, new DefaultCodec());
                    }
                };
            }
        };
    }
}
