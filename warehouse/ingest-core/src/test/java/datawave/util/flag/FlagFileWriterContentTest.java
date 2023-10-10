package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

public class FlagFileWriterContentTest {

    private static final String EXPECTED_SCRIPT = "bin/ingest/bulk-ingest.sh";
    private static final String EXPECTED_DATAWAVE_HOME = "target/test";
    private static final String EXPECTED_NUM_REDUCERS = " 10";
    private static final String EXPECTED_INPUT_FORMAT_ARG = " -inputFormat datawave.ingest.input.reader.event.EventSequenceFileInputFormat";
    private static final String FLAG_MARKER = "XXXX--test-marker--XXXX";
    // See FlagFileWithMarker.flag
    private static final String EXPECTED_FIRST_LINE_WITH_MARKER = EXPECTED_DATAWAVE_HOME + File.separatorChar + EXPECTED_SCRIPT + " ${JOB_FILE}"
                    + EXPECTED_NUM_REDUCERS + EXPECTED_INPUT_FORMAT_ARG + " -inputFileLists -inputFileListMarker " + FLAG_MARKER + " ";
    private static final String EXPECTED_BEGINNING = EXPECTED_FIRST_LINE_WITH_MARKER + "\n" + FLAG_MARKER;

    private FlagFileTestSetup flagFileTestSetup;
    private FlagMakerConfig flagMakerConfig;
    private FlagDataTypeConfig dataTypeConfig;
    private Collection<InputFile> inputFiles;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception {
        flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig()
                        .withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        flagMakerConfig = flagFileTestSetup.getFlagMakerConfig();
        this.dataTypeConfig = flagFileTestSetup.getInheritedDataTypeConfig();
        this.inputFiles = createInputFiles();
    }

    @After
    public void cleanup() throws IOException {
        flagFileTestSetup.deleteTestDirectories();
    }

    @Test
    public void writeProducesExpectedFlagFileWithMarker() throws Exception {
        dataTypeConfig.setFileListMarker(FLAG_MARKER);

        File flag = new FlagFileWriter(flagMakerConfig).write(inputFiles, dataTypeConfig, flagMakerConfig.getBaseHDFSDir());

        // @formatter:off
		FlagFileContentExpectations flagFileContentExpectations = new FlagFileContentExpectations()
				.withBeginning(EXPECTED_BEGINNING + "\n").withFiles(inputFiles)
				.withEnding("\n").withFileOrdering(true).withFileMarker(true);
		// @formatter:on

        flagFileContentExpectations.assertFlagFileContents(flag);
    }

    @Test
    public void writeFlagProducesExpectedFlagFileWithMarker() throws Exception {
        dataTypeConfig.setFileListMarker(FLAG_MARKER);

        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        File flag = FlagFileTestInspector.getOnlyFlagFile(this.flagMakerConfig);
        // @formatter:off
		FlagFileContentExpectations flagFileContentExpectations = new FlagFileContentExpectations()
				.withBeginning(EXPECTED_BEGINNING).withFiles(inputFiles)
				.withEnding("\n").withFileOrdering(false).withFileMarker(true);
		// @formatter:on

        flagFileContentExpectations.assertFlagFileContents(flag);
    }

    @Test
    public void writeProducesExpectedFlagFileWithoutMarker() throws Exception {
        // does not set flagMarker

        File flag = new FlagFileWriter(flagMakerConfig).write(inputFiles, dataTypeConfig, flagMakerConfig.getBaseHDFSDir());

        // @formatter:off
		FlagFileContentExpectations flagFileContentExpectations = new FlagFileContentExpectations()
				.withBeginning(
						EXPECTED_DATAWAVE_HOME + File.separatorChar + EXPECTED_SCRIPT + " ")
				.withEnding(
						EXPECTED_NUM_REDUCERS + EXPECTED_INPUT_FORMAT_ARG + " \n")
                .withFiles(inputFiles)
				.withFileOrdering(true).withFileMarker(false);
		// @formatter:on

        flagFileContentExpectations.assertFlagFileContents(flag);
    }

    @Test
    public void writeFlagProducesExpectedFlagFileWithoutMarker() throws Exception {
        // does not set flagMarker

        new FlagFileWriter(flagMakerConfig).writeFlagFile(dataTypeConfig, inputFiles);

        // @formatter:off
		FlagFileContentExpectations flagFileContentExpectations = new FlagFileContentExpectations()
				.withBeginning(
						EXPECTED_DATAWAVE_HOME + File.separatorChar + EXPECTED_SCRIPT)
				.withEnding(
						EXPECTED_NUM_REDUCERS + EXPECTED_INPUT_FORMAT_ARG + " \n")
                .withFiles(inputFiles)
				.withFileOrdering(false).withFileMarker(false);
		// @formatter:on

        File flag = FlagFileTestInspector.getOnlyFlagFile(this.flagMakerConfig);
        flagFileContentExpectations.assertFlagFileContents(flag);
    }

    private TreeSet<InputFile> createInputFiles() throws IOException {
        // creates 10 files: 5 in foo, 5 in bar
        flagFileTestSetup.withFilesPerDay(5).withNumDays(1).createTestFiles();
        TreeSet<InputFile> sortedFiles = new TreeSet<>(InputFile.FIFO);
        sortedFiles.addAll(FlagFileTestInspector.listSortedInputFiles(this.flagMakerConfig, flagFileTestSetup.getFileSystem()));
        // verify file creation
        assertNotNull(sortedFiles);
        assertEquals(10, sortedFiles.size());
        return sortedFiles;
    }
}
