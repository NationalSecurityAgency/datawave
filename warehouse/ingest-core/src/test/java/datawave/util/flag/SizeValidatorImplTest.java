package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
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

    private FlagMakerConfig flagMakerConfig;
    private Configuration configuration;
    private FlagDataTypeConfig dataTypeConfig;
    private FlagFileTestSetup flagFileTestSetup;

    @Before
    public void before() throws Exception {
        this.configuration = new Configuration(false); // do not load defaults

        this.flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig();
        this.flagMakerConfig = flagFileTestSetup.fmc;
        this.dataTypeConfig = this.flagFileTestSetup.getInheritedDataTypeConfig();
    }

    @Test
    public void testBaselineSize() {
        verifyExpectedVariancePerFlagFile(0);
    }

    @Test
    public void countsExtraArgsOncePerFlag() {
        this.dataTypeConfig.setExtraIngestArgs(" -other -args");
        verifyExpectedVariancePerFlagFile(" -other -args".length());
    }

    @Test
    public void countsLengthOfNumReducers() {
        this.dataTypeConfig.setReducers(1000); // default is 10, so two digits
        verifyExpectedVariancePerFlagFile(2);

        this.dataTypeConfig.setReducers(1); // default is 10, so two digits
        verifyExpectedVariancePerFlagFile(-1);
    }

    @Test
    public void countsInputFormatName() {
        this.dataTypeConfig.setInputFormat(this.getClass()); // default is
                                                             // EventSequenceFileInputFormat
        verifyExpectedVariancePerFlagFile(this.getClass().getName().length() - EventSequenceFileInputFormat.class.getName().length());
    }

    @Test
    public void countsDatawaveHomeLengthOncePerFlag() {
        int originalLength = this.flagMakerConfig.getDatawaveHome().length();
        this.flagMakerConfig.setDatawaveHome("");
        verifyExpectedVariancePerFlagFile(-1 * originalLength); // sizes should
                                                                // be shorter by
                                                                // originalLength

        String longerDatawaveHome = "/opt/datawave/non/empty/";
        this.flagMakerConfig.setDatawaveHome(longerDatawaveHome);
        verifyExpectedVariancePerFlagFile(longerDatawaveHome.length() - originalLength);
    }

    @Test
    public void countsScriptLengthOncePerFlag() {
        int originalLength = this.flagMakerConfig.getDatawaveHome().length();
        this.flagMakerConfig.setDatawaveHome("");
        // verifyExpectedVariancePerFlagFile(-1 * originalLength); // sizes should
        // be shorter by
        // originalLength

        String longerDatawaveHome = "/opt/datawave/non/empty/";
        this.flagMakerConfig.setDatawaveHome(longerDatawaveHome);
        verifyExpectedVariancePerFlagFile(longerDatawaveHome.length() - originalLength);
    }

    @Test
    public void countsInputFileLength() {
        HashSet<InputFile> setWithLongerBaseDir = InputFileSets.withNewBaseDir(InputFileSets.MULTIPLE_FILES, "longerBaseDir");
        int sizeOfOriginalBaseDir = 1;
        int addedSize = ("longerBaseDir".length() - sizeOfOriginalBaseDir) * InputFileSets.MULTIPLE_FILES.size();
        verifyMaxSizeValidation(BASELINE_MULTIPLE_FILES + addedSize, setWithLongerBaseDir);
    }

    @Test
    public void countsMarkerAdditionsPerFlagFile() {
        int addedLength = " ${JOB_NAME} -inputFileLists -inputFileListMarker".length();

        this.dataTypeConfig.setFileListMarker(".");
        int shorterMarkerAddedLength = addedLength + ".".length() + "\n.\n".length();
        verifyExpectedVariancePerFlagFile(shorterMarkerAddedLength); // sizes
                                                                     // should
                                                                     // be
                                                                     // longer
                                                                     // by
                                                                     // minimum
                                                                     // length

        this.dataTypeConfig.setFileListMarker("------fileMarker-------");
        int longerMarkerAddedLength = addedLength + "------fileMarker-------".length() + "\n------fileMarker-------\n".length();

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
        verifyMaxSizeValidation(BASELINE_NO_FILES + expectedSizeVariance, InputFileSets.EMPTY_FILES);
        verifyMaxSizeValidation(BASELINE_ONE_FILE + expectedSizeVariance, InputFileSets.SINGLE_FILE);
        verifyMaxSizeValidation(BASELINE_MULTIPLE_FILES + expectedSizeVariance, InputFileSets.MULTIPLE_FILES);
        verifyMaxSizeValidation(BASELINE_MANY_FILES + expectedSizeVariance, InputFileSets.MANY_FILES);
    }

    private void verifyMaxSizeValidation(int expectedSize, HashSet<InputFile> inputFiles) {
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
