package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;
import datawave.util.flag.config.FlagMakerConfigUtilityTest;

/**
 * Assists with the loading of a sample flag maker configuration file, as well as the generation of test files that align with the configuration.
 */
public class FlagFileTestSetup {
    private static final Logger LOG = LoggerFactory.getLogger(FlagFileTestSetup.class);
    private static final String TEST_CONFIG = FlagMakerConfigUtilityTest.TEST_CONFIG;

    private final FlagFileInputStructure flagFileInputStructure;
    private final FlagMakerTimestampTracker timestampTracker;

    private FlagMakerConfig fmc;
    private final FileSystem fs;
    private String subDirectoryName;
    private String fileNameSuffix = null;
    private boolean usePredicableInputFilenames = false;
    private int numFilesPerDay = 1;
    private int numDays = 1;

    public FlagFileTestSetup() {
        timestampTracker = new FlagMakerTimestampTracker();
        flagFileInputStructure = new FlagFileInputStructure(this, timestampTracker);

        try {
            fs = FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FlagFileTestSetup withTestFlagMakerConfig() throws Exception {
        return withConfig(FlagMakerConfigUtility.getXmlObject(FlagMakerConfig.class, new FileReader(TEST_CONFIG)));
    }

    public FlagFileTestSetup withTestNameForDirectories(String testName) throws IOException {
        this.subDirectoryName = testName;
        if (null != this.fmc) {
            if (this.fmc.getBaseHDFSDir().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified test directory name");
            }
            this.fmc.setBaseHDFSDir(this.fmc.getBaseHDFSDir().replace("target", "target/" + this.subDirectoryName));
            flagFileInputStructure.createDirectory(fmc.getBaseHDFSDir());
            LOG.info("Set base HDFS directory to " + this.fmc.getBaseHDFSDir());

            if (this.fmc.getFlagFileDirectory().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified test directory name");
            }
            this.fmc.setFlagFileDirectory(this.fmc.getFlagFileDirectory().replace("target", "target/" + subDirectoryName));
            LOG.info("Set flag file directory to " + this.fmc.getFlagFileDirectory());
            flagFileInputStructure.createDirectory(fmc.getFlagFileDirectory());

            if (this.fmc.getFlagMetricsDirectory().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified metrics directory name");
            }
            this.fmc.setFlagMetricsDirectory(this.fmc.getFlagMetricsDirectory().replace("target", "target/" + subDirectoryName));
            LOG.info("Set flag file directory to " + this.fmc.getFlagMetricsDirectory());
            flagFileInputStructure.createDirectory(fmc.getFlagMetricsDirectory());
        }
        return this;
    }

    public FlagFileTestSetup withConfig(FlagMakerConfig fmc) throws IOException {
        this.fmc = fmc;

        if (null != subDirectoryName) {
            withTestNameForDirectories(subDirectoryName);
        } else {
            flagFileInputStructure.createDirectory(fmc.getBaseHDFSDir());
        }

        return this;
    }

    public FlagFileTestSetup withFilesPerDay(int filesPerDay) {
        this.numFilesPerDay = filesPerDay;
        return this;
    }

    public FlagFileTestSetup withNumDays(int numDays) {
        this.numDays = numDays;
        return this;
    }

    public FlagFileTestSetup withTimeoutMilliSecs(long timeout) throws Exception {
        if (fmc == null) {
            this.withTestFlagMakerConfig();
        }
        fmc.setTimeoutMilliSecs(timeout);
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            fc.setTimeoutMilliSecs(timeout);
        }
        return this;
    }

    public FlagFileTestSetup withFlagCountThreshold(int flagCountThreshold) throws Exception {
        if (fmc == null) {
            this.withTestFlagMakerConfig();
        }
        fmc.setFlagCountThreshold(flagCountThreshold);
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            fc.setFlagCountThreshold(flagCountThreshold);
        }
        return this;
    }

    public FlagFileTestSetup withMaxFlags(int max) throws Exception {
        if (fmc == null) {
            this.withTestFlagMakerConfig();
        }
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            fc.setMaxFlags(max);
        }
        return this;
    }

    public FlagFileTestSetup withTestFileNameSuffix(String suffix) {
        this.fileNameSuffix = suffix;
        return this;
    }

    public FlagFileTestSetup withPredicableInputFilenames() {
        this.usePredicableInputFilenames = true;
        return this;
    }

    protected String getTestFileNameSuffix() {
        return this.fileNameSuffix;
    }

    protected int getNumDays() {
        return numDays;
    }

    protected int getNumFilesPerDay() {
        return numFilesPerDay;
    }

    public FlagMakerConfig getFlagMakerConfig() {
        return fmc;
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    public boolean arePredicableInputFilenames() {
        return this.usePredicableInputFilenames;
    }

    public void createTestFiles() throws IOException {
        flagFileInputStructure.createEmptyDirectories();
        flagFileInputStructure.createAdditionalTestFiles();
    }

    public void createAdditionalTestFiles() throws IOException {
        this.flagFileInputStructure.createAdditionalTestFiles();
    }

    public Collection<String> getNamesOfCreatedFiles() {
        return new ArrayList<>(flagFileInputStructure.getNamesOfCreatedFiles());
    }

    public void deleteTestDirectories() throws IOException {
        flagFileInputStructure.deleteTestDirectories();
    }

    public void createTrackedDirectoriesForInputFile(InputFile inputFile) throws IOException {
        final Path[] directories = {inputFile.getFlagged(), inputFile.getFlagging(), inputFile.getLoaded()};
        for (final Path directory : directories) {
            final Path parentDirectory = directory.getParent();
            if (!fs.mkdirs(parentDirectory)) {
                throw new IllegalStateException("unable to create tracked directory (" + parentDirectory + ")");
            }
        }
    }

    public FlagDataTypeConfig getInheritedDataTypeConfig() {
        // causes datatype configs, like foo, to inherit properties from default Config
        fmc.validate();
        // typically foo is the first
        FlagDataTypeConfig dataTypeConfig = fmc.getFlagConfigs().get(0);
        // verify set up
        assertEquals(10L, dataTypeConfig.getReducers());
        assertFalse(dataTypeConfig.isLifo());
        return dataTypeConfig;
    }

    public long getMinFolderTime() {
        return this.timestampTracker.minFolderTime;
    }

    public long getMaxFolderTime() {
        return this.timestampTracker.maxFolderTime;
    }

    public long getMinLastModified() {
        return this.timestampTracker.minLastModified;
    }

    public long getMaxLastModified() {
        return this.timestampTracker.maxLastModified;
    }

    public Collection<Long> getLastModifiedTimes() {
        return timestampTracker.fileLastModifiedTimes;
    }
}
