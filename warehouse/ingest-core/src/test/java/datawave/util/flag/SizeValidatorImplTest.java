package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.input.reader.event.EventSequenceFileInputFormat;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

public class SizeValidatorImplTest {

    private static final String HADOOP_1_NAME = "mapreduce.job.counters.limit";
    private static final String HADOOP_2_NAME = "mapreduce.job.counters.max";

    private static final int BASELINE_NO_FILES = 119;
    private static final int BASELINE_ONE_FILE = 147;
    private static final int BASELINE_MULTIPLE_FILES = 679;
    private static final int BASELINE_MANY_FILES = 280420;
    private static final boolean DO_NOT_LOAD_DEFAULTS = false;

    private Configuration configuration;
    private FlagMakerConfig flagMakerConfig;
    private FlagDataTypeConfig dataTypeConfig;

    @Before
    public void before() throws Exception {
        this.configuration = new Configuration(DO_NOT_LOAD_DEFAULTS);

        FlagFileTestSetup flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig();
        this.flagMakerConfig = flagFileTestSetup.getFlagMakerConfig();
        this.dataTypeConfig = flagFileTestSetup.getInheritedDataTypeConfig();
    }

    @Test
    public void testBaselineSize() {
        verifyExpectedVariancePerFlagFile(0);
    }

    @Test
    public void countsExtraArgsOncePerFlag() {
        String extraIngestArgs = " -other -args";
        this.dataTypeConfig.setExtraIngestArgs(extraIngestArgs);
        verifyExpectedVariancePerFlagFile(extraIngestArgs.length());
    }

    @Test
    public void countsLengthOfNumReducers() {
        // default is 10, so expect two digit increase
        this.dataTypeConfig.setReducers(1000);
        verifyExpectedVariancePerFlagFile(2);

        // default is 10, so expect one digit decrease
        this.dataTypeConfig.setReducers(1);
        verifyExpectedVariancePerFlagFile(-1);
    }

    @Test
    public void countsInputFormatName() {
        // default is EventSequenceFileInputFormat
        Class<FileInputFormat> inputFormat = FileInputFormat.class;
        this.dataTypeConfig.setInputFormat(inputFormat);
        int expectedSizeVariance = inputFormat.getName().length() - EventSequenceFileInputFormat.class.getName().length();
        verifyExpectedVariancePerFlagFile(expectedSizeVariance);
    }

    @Test
    public void countsDatawaveHomeLengthOncePerFlag() {
        int originalLength = this.flagMakerConfig.getDatawaveHome().length();
        this.flagMakerConfig.setDatawaveHome("");
        // sizes should be shorter by originalLength
        verifyExpectedVariancePerFlagFile(-1 * originalLength);

        String longerDatawaveHome = "/opt/datawave/non/empty/";
        this.flagMakerConfig.setDatawaveHome(longerDatawaveHome);
        verifyExpectedVariancePerFlagFile(longerDatawaveHome.length() - originalLength);
    }

    @Test
    public void countsScriptLengthOncePerFlag() {
        int originalLength = this.dataTypeConfig.getScript().length();
        this.dataTypeConfig.setScript("");
        // sizes should be shorter by originalLength
        verifyExpectedVariancePerFlagFile(-1 * originalLength);

        String longerScript = "exceedingly-lengthy-script-name.sh";
        this.dataTypeConfig.setScript(longerScript);
        verifyExpectedVariancePerFlagFile(longerScript.length() - originalLength);
    }

    @Test
    public void countsInputFileLength() {
        int sizeOfOriginalBaseDir = 1;

        String longerBaseDir = "longerBaseDir";
        HashSet<InputFile> inputFileSetWithLongerBaseDir = InputFileSets.withNewBaseDir(InputFileSets.MULTIPLE_FILES, longerBaseDir);

        int addedSize = (longerBaseDir.length() - sizeOfOriginalBaseDir) * InputFileSets.MULTIPLE_FILES.size();
        verifyMaxSize(BASELINE_MULTIPLE_FILES + addedSize, inputFileSetWithLongerBaseDir);
    }

    @Test
    public void countsMarkerAdditionsPerFlagFile() {
        int lengthOfAddedArgumentsForFlagsWithMarker = " ${JOB_NAME} -inputFileLists -inputFileListMarker".length();

        String shorterMarker = ".";
        this.dataTypeConfig.setFileListMarker(shorterMarker);
        int shorterMarkerAddedLength = lengthOfAddedArgumentsForFlagsWithMarker + shorterMarker.length() + ("\n" + shorterMarker + "\n").length();
        verifyExpectedVariancePerFlagFile(shorterMarkerAddedLength);

        String longerMarker = "------fileMarker-------";
        this.dataTypeConfig.setFileListMarker(longerMarker);
        int longerMarkerAddedLength = lengthOfAddedArgumentsForFlagsWithMarker + longerMarker.length() + ("\n" + longerMarker + "\n").length();

        verifyExpectedVariancePerFlagFile(longerMarkerAddedLength);
    }

    @Test
    public void honorsHadoopOneCounterLimitProperty() {
        // ensure the max file limit is not exceeded
        flagMakerConfig.setMaxFileLength(Integer.MAX_VALUE);

        SizeValidatorImpl sizeValidator = new SizeValidatorImpl(this.configuration, this.flagMakerConfig);

        for (Collection<InputFile> fileSet : InputFileSets.VARIOUS_SETS_OF_FILES) {
            assertPropertyLimitForFileSet(sizeValidator, HADOOP_1_NAME, fileSet);
        }
    }

    @Test
    public void honorsHadoopTwoCounterLimitProperty() {
        // ensure the max file limit is not exceeded
        flagMakerConfig.setMaxFileLength(Integer.MAX_VALUE);

        SizeValidatorImpl sizeValidator = new SizeValidatorImpl(this.configuration, this.flagMakerConfig);

        for (Collection<InputFile> fileSet : InputFileSets.VARIOUS_SETS_OF_FILES) {
            assertPropertyLimitForFileSet(sizeValidator, HADOOP_2_NAME, fileSet);
        }
    }

    @Test
    public void ignoresCountersIfLimitMissing() {
        // ensure the max file limit is not exceeded
        flagMakerConfig.setMaxFileLength(Integer.MAX_VALUE);
        SizeValidatorImpl sizeValidator = new SizeValidatorImpl(this.configuration, this.flagMakerConfig);

        for (Collection<InputFile> fileSet : InputFileSets.VARIOUS_SETS_OF_FILES) {
            assertTrue("Expected to never exceed counter limit for file set " + fileSet.size(), sizeValidator.isValidSize(dataTypeConfig, fileSet));
        }
    }

    private void verifyExpectedVariancePerFlagFile(int expectedSizeVariance) {
        verifyMaxSize(BASELINE_NO_FILES + expectedSizeVariance, InputFileSets.EMPTY_FILES);
        verifyMaxSize(BASELINE_ONE_FILE + expectedSizeVariance, InputFileSets.SINGLE_FILE);
        verifyMaxSize(BASELINE_MULTIPLE_FILES + expectedSizeVariance, InputFileSets.MULTIPLE_FILES);
        verifyMaxSize(BASELINE_MANY_FILES + expectedSizeVariance, InputFileSets.MANY_FILES);
    }

    private void verifyMaxSize(int expectedSize, HashSet<InputFile> inputFiles) {
        // make max file length too small
        this.flagMakerConfig.setMaxFileLength(expectedSize - 1);
        SizeValidatorImpl sizeValidator = new SizeValidatorImpl(this.configuration, this.flagMakerConfig);
        assertEquals("Unexpected calculated file size", expectedSize, sizeValidator.calculateFlagFileSize(this.dataTypeConfig, inputFiles));
        assertFalse("Validation did not detect excessive size", sizeValidator.isValidSize(this.dataTypeConfig, inputFiles));

        // make max file length just right
        this.flagMakerConfig.setMaxFileLength(expectedSize);
        sizeValidator = new SizeValidatorImpl(this.configuration, this.flagMakerConfig);
        assertEquals("Unexpected change to calculated file size", expectedSize, sizeValidator.calculateFlagFileSize(this.dataTypeConfig, inputFiles));
        assertTrue("Validation did not detect valid size", sizeValidator.isValidSize(this.dataTypeConfig, inputFiles));
    }

    private void assertPropertyLimitForFileSet(SizeValidatorImpl sizeValidator, String propertyName, Collection<InputFile> fileSet) {
        int expectedNumberOfCountersUsed = 2 + 2 * fileSet.size();

        // set limit to exact size
        this.configuration.setInt(propertyName, expectedNumberOfCountersUsed);
        assertTrue("Expected not to exceed counter limit: " + expectedNumberOfCountersUsed, sizeValidator.isValidSize(dataTypeConfig, fileSet));

        // set too small
        int tooSmall = expectedNumberOfCountersUsed - 1;
        this.configuration.setInt(propertyName, tooSmall);
        assertFalse("Expected to exceed counter limit: " + tooSmall, sizeValidator.isValidSize(dataTypeConfig, fileSet));
    }
}