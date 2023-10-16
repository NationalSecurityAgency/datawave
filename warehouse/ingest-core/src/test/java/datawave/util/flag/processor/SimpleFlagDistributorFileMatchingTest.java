package datawave.util.flag.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

import datawave.util.flag.FlagFileTestSetup;
import datawave.util.flag.FlagMakerTest;
import datawave.util.flag.InMemoryStubFileSystem;
import datawave.util.flag.InputFile;
import datawave.util.flag.SizeValidatorImpl;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

public class SimpleFlagDistributorFileMatchingTest {
    private static final String BASE_DIRECTORY = FlagMakerTest.CONFIG_BASE_HDFS_DIR + "foo";
    private static final boolean NOT_MUST_HAVE_MAX = false;
    private static final String PATTERN_FILE_NAME = "[0-9a-zA-Z]*[0-9a-zA-Z]";
    private static final String PATTERN_3_DIRS_FILE_NAME = "2*/*/*/" + PATTERN_FILE_NAME;
    private static final String PATTERN_4_DIRS_FILE_NAME = "2*/*/*/*/" + PATTERN_FILE_NAME;
    private static final SizeValidator ALWAYS_TRUE_SIZE_VALIDATOR = (fc, files) -> true;

    private FlagDataTypeConfig flagDataTypeConfig;
    private SimpleFlagDistributor flagDistributor;
    private FlagFileTestSetup flagFileTestSetup;

    @Before
    public void setup() throws Exception {
        this.flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig();
        FlagMakerConfig fmc = flagFileTestSetup.getFlagMakerConfig();
        fmc.validate(); // modifies FlagDataTypeConfig.folders by changing from
                        // String to List<String>, splitting on comma

        verifyTestPreconditions(fmc);

        this.flagDistributor = new SimpleFlagDistributor(fmc);
        this.flagDataTypeConfig = fmc.getFlagConfigs().get(0);
        this.flagDataTypeConfig.setMaxFlags(1);
    }

    private void verifyTestPreconditions(FlagMakerConfig fmc) {
        List<String> filePatterns = fmc.getFilePatterns();
        Preconditions.checkArgument(filePatterns.contains(PATTERN_3_DIRS_FILE_NAME));
        Preconditions.checkArgument(filePatterns.contains(PATTERN_4_DIRS_FILE_NAME));
        Preconditions.checkArgument(2 == filePatterns.size(), "Expected two patterns, got " + filePatterns);

        List<FlagDataTypeConfig> flagDataTypeConfigs = fmc.getFlagConfigs();
        Preconditions.checkArgument(1 == flagDataTypeConfigs.size(), "Expected just one datatype config");
        Preconditions.checkArgument(2 == flagDataTypeConfigs.get(0).getFolders().size(), "Expected two folders");
    }

    @Test
    public void matchesMinimumLengthPatterns() {
        // 3 Levels deep
        // 2*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]
        assertFound(BASE_DIRECTORY + "/2/0/0/00");
        assertFound(BASE_DIRECTORY + "/2/0/-/x0");
        assertFound(BASE_DIRECTORY + "/2/-/0/X0");
        assertFound(BASE_DIRECTORY + "/2/0/0/90");
        assertFound(BASE_DIRECTORY + "/2/$/0/00");
        assertFound(BASE_DIRECTORY + "/2/g/0/xx");
        assertFound(BASE_DIRECTORY + "/2/>/0/XX");
        assertFound(BASE_DIRECTORY + "/2/0/0/99");

        // 4 Levels deep
        // 2*/*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]
        assertFound(BASE_DIRECTORY + "/2/0/0/0/00");
        assertFound(BASE_DIRECTORY + "/2/0/-/0/x0");
        assertFound(BASE_DIRECTORY + "/2/-/0/0/X0");
        assertFound(BASE_DIRECTORY + "/2/0/0/-/90");
        assertFound(BASE_DIRECTORY + "/2/$/0/0/00");
        assertFound(BASE_DIRECTORY + "/2/g/0/-/xx");
        assertFound(BASE_DIRECTORY + "/2/>/0/0/XX");
        assertFound(BASE_DIRECTORY + "/2/0/0/-/99");
    }

    @Test
    public void acceptsLongerNames() {
        // 3 levels deep
        assertFound(BASE_DIRECTORY + "/2000/99/99/my file .txt");

        // 4 levels deep
        assertFound(BASE_DIRECTORY + "/2000/99/99/99999999/my file .txt");
    }

    @Test
    public void rejectsTooShortFilename() {
        // Filename must have at least one character
        // +0: [0-9a-zA-Z]*
        // +1: [0-9a-zA-Z]

        // 3 levels down
        assertIgnored(BASE_DIRECTORY + "/2/0/0/0");

        // 4 levels down
        assertIgnored(BASE_DIRECTORY + "/2/0/0/0/0");
    }

    @Test
    public void rejectsLevel2File() {
        // not enough directories
        assertIgnored(BASE_DIRECTORY + "/20/0/4xyz4");
    }

    @Test
    public void rejectsPrefixMismatch() {
        // top level directory must begin with 2
        assertIgnored(BASE_DIRECTORY + "/1/0/0/00");
        assertIgnored(BASE_DIRECTORY + "/1/0/0/0/00");
        assertIgnored(BASE_DIRECTORY + "/12/0/0/00");
        assertIgnored(BASE_DIRECTORY + "/12/0/0/0/00");
    }

    @Test
    public void rejectsFileInDifferentBaseDirectory() {
        // otherwise matching file rejected when appearing under ignored top level directory
        assertIgnored("/elsewhere" + "/2/0/0/00");
    }

    @Test
    public void rejectsLevel5File() {
        // too many levels deep
        assertIgnored(BASE_DIRECTORY + "/2/0/0/0/0/00");
        assertIgnored(BASE_DIRECTORY + "/2/1/0/0/0/00");
    }

    @Test
    public void rejectsTooShortTopDirectoryName() {
        // directory name empty
        assertIgnored(BASE_DIRECTORY + "/2//0/0");
    }

    private void assertFound(String relativePathStr) {
        try {
            loadFileIntoDistributor(relativePathStr);

            boolean flagMakerHasFile = this.flagDistributor.hasNext(NOT_MUST_HAVE_MAX);
            assertTrue("FlagMaker didn't accept expected file: " + relativePathStr, flagMakerHasFile);

            Collection<InputFile> result = this.flagDistributor.next(new SizeValidatorImpl(new Configuration(), this.flagFileTestSetup.getFlagMakerConfig()));
            assertEquals("expected only single file", 1, result.size());

            String actualFileName = result.iterator().next().getPath().toString();
            assertTrue("File name unexpectedly altered: \n" + actualFileName + "\n" + relativePathStr, actualFileName.endsWith(relativePathStr));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void assertIgnored(String relativePathStr) {
        try {
            loadFileIntoDistributor(relativePathStr);

            boolean flagMakerHasFile = this.flagDistributor.hasNext(NOT_MUST_HAVE_MAX);
            if (flagMakerHasFile) {
                InputFile firstFile = this.flagDistributor.next(ALWAYS_TRUE_SIZE_VALIDATOR).iterator().next();
                fail("Didn't expect to match.  Found: " + firstFile.getPath().toString());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void loadFileIntoDistributor(String relativePathStr) throws IOException {
        // simulate filesystem with one file
        InMemoryStubFileSystem fakeFileSystem = new InMemoryStubFileSystem("hdfs");
        fakeFileSystem.addFile(relativePathStr);

        // register filesystem with flag maker
        this.flagDistributor.setFileSystem(fakeFileSystem);

        // cause flag distributor to look for files
        this.flagDistributor.loadFiles(this.flagDataTypeConfig);
    }
}
