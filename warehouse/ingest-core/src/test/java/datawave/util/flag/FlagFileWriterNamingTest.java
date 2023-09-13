package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import datawave.util.flag.config.FlagDataTypeConfig;

public class FlagFileWriterNamingTest {
    private static final String POOL_NAME = "onehr";
    private static final String FLAG_DATA_TYPE = "foo";

    private String flagFilePath;

    private Collection<InputFile> inputFiles;

    @Rule
    public TestName testName = new TestName();

    FlagFileTestSetup flagFileTestSetup;

    @Before
    public void before() throws Exception {
        flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig()
                        .withTestNameForDirectories(this.getClass().getName() + "_" + testName.getMethodName());
        FlagDataTypeConfig dataTypeConfig = flagFileTestSetup.getInheritedDataTypeConfig();
        inputFiles = createInputFiles(flagFileTestSetup);

        // write a flag file with FlagFileWriter
        new FlagFileWriter(flagFileTestSetup.fmc).writeFlagFile(dataTypeConfig, inputFiles);

        // capture the name of the flag file
        this.flagFilePath = FlagFileTestHelper.getFlagFilePath(flagFileTestSetup.fmc);
    }

    @After
    public void cleanup() throws IOException {
        flagFileTestSetup.deleteTestDirectories();
    }

    private TreeSet<InputFile> createInputFiles(FlagFileTestSetup flagFileTestSetup) throws IOException {
        flagFileTestSetup.withFilesPerDay(5).withNumDays(1).createTestFiles(); // creates
                                                                               // 10
                                                                               // files:
                                                                               // 5
                                                                               // in
                                                                               // foo,
                                                                               // 5
                                                                               // in
                                                                               // bar
        TreeSet<InputFile> sortedFiles = new TreeSet<>(InputFile.FIFO);
        sortedFiles.addAll(FlagFileTestHelper.listSortedInputFiles(flagFileTestSetup.fmc, flagFileTestSetup.fs));
        // verify file creation
        assertNotNull(sortedFiles);
        assertEquals(10, sortedFiles.size());
        return sortedFiles;
    }

    private void assertFlagFileNameContains(String expectedSubstring) {
        assertTrue(flagFilePath + " does not contain " + expectedSubstring, flagFilePath.contains(expectedSubstring));
    }

    @Test
    public void writesToFlagDirectory() {
        assertFlagFileNameContains("target/datawave.util.flag.FlagFileWriterNamingTest_writesToFlagDirectory/test/flags/");
    }

    @Test
    public void includesIngestPoolInFlagFileName() {
        assertFlagFileNameContains(POOL_NAME);
    }

    @Test
    public void includesDataNameInFlagFileName() {
        assertFlagFileNameContains(FLAG_DATA_TYPE);
    }

    @Test
    public void includesFirstFileNameInFlagFileName() {
        InputFile firstInputFile = inputFiles.iterator().next();
        assertTrue(flagFilePath + " does not contain any files from " + inputFiles.stream().map(InputFile::getFileName).collect(Collectors.joining("\n")),
                        flagFilePath.contains(firstInputFile.getFileName()));
    }

    @Test
    public void includesNumberOfInputsFilesInFlagFileName() {
        assertFlagFileNameContains("+" + inputFiles.size() + ".");
    }

    @Test
    public void fileNameMatchesPattern() {
        // patterns for expected components
        String directoryPattern = "target/datawave.util.flag.FlagFileWriterNamingTest_fileNameMatchesPattern/test/flags".replaceAll("/", "\\\\\\/");
        String timePattern = "[0-9]{10}\\.[0-9]{2}";
        String firstFileNamePattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
        String numFilesPattern = Integer.toString(inputFiles.size());

        // combined pattern
        String regexPattern = directoryPattern + "/" + timePattern + "_" + POOL_NAME + "_" + FLAG_DATA_TYPE + "_" + firstFileNamePattern + "\\+"
                        + numFilesPattern + "\\.flag";

        assertTrue(regexPattern + " did not match: \n" + this.flagFilePath, Pattern.compile(regexPattern).matcher(this.flagFilePath).matches());
    }
}
