package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;

/**
 * Simplifies the loading of a sample flag maker configuration file as well as the generation of test files that align with the configuration.
 */
public class FlagFileTestSetup {
    private static final Logger LOG = LoggerFactory.getLogger(FlagFileTestSetup.class);

    private static final String TEST_CONFIG = "target/test-classes/TestFlagMakerConfig.xml";
    private final FlagFileInputStructure flagFileInputStructure;
    private final FlagMakerTimestampTracker timeTracker;

    private boolean usePredicableInputFilenames = false;
    private String subDirectoryName;

    private int filesPerDay = 1;
    private int numDays = 1;

    private FlagMakerConfig fmc;
    private final FileSystem fs;

    private String suffix = null;

    public FlagFileTestSetup() {
        timeTracker = new FlagMakerTimestampTracker();
        flagFileInputStructure = new FlagFileInputStructure(this, timeTracker);

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
            flagFileInputStructure.createDirectory(fmc.getBaseHDFSDir(), "base HDFS");
            LOG.info("Set base HDFS directory to " + this.fmc.getBaseHDFSDir());

            if (this.fmc.getFlagFileDirectory().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified test directory name");
            }
            this.fmc.setFlagFileDirectory(this.fmc.getFlagFileDirectory().replace("target", "target/" + subDirectoryName));
            LOG.info("Set flag file directory to " + this.fmc.getFlagFileDirectory());
            flagFileInputStructure.createDirectory(fmc.getFlagFileDirectory(), "flag");

            if (this.fmc.getFlagMetricsDirectory().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified metrics directory name");
            }
            this.fmc.setFlagMetricsDirectory(this.fmc.getFlagMetricsDirectory().replace("target", "target/" + subDirectoryName));
            LOG.info("Set flag file directory to " + this.fmc.getFlagMetricsDirectory());
            flagFileInputStructure.createDirectory(fmc.getFlagMetricsDirectory(), "flag");
        }
        return this;
    }

    public FlagFileTestSetup withConfig(FlagMakerConfig fmc) throws IOException {
        this.fmc = fmc;

        if (null != subDirectoryName) {
            withTestNameForDirectories(subDirectoryName);
        } else {
            flagFileInputStructure.createDirectory(fmc.getBaseHDFSDir(), "base HDFS");
        }

        return this;
    }

    public FlagFileTestSetup withFilesPerDay(int filesPerDay) {
        this.filesPerDay = filesPerDay;
        return this;
    }

    public FlagFileTestSetup withNumDays(int numDays) {
        this.numDays = numDays;
        return this;
    }

    public FlagFileTestSetup withTimeoutMilliSecs(int timeout) throws Exception {
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
        this.suffix = suffix;
        return this;
    }

    public FlagFileTestSetup withPredicableInputFilenames() {
        this.usePredicableInputFilenames = true;
        return this;
    }

    protected String getTestFileNameSuffix() {
        return this.suffix;
    }

    protected int getNumDays() {
        return numDays;
    }

    protected int getFilesPerDay() {
        return filesPerDay;
    }


    public FlagMakerConfig getFlagMakerConfig() {
        return fmc;
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    public boolean isPredicableInputFilenames() {
        return this.usePredicableInputFilenames;
    }

    public void createTestFiles() throws IOException {
        flagFileInputStructure.emptyDirectories();
        flagFileInputStructure.createAdditionalTestFiles();
    }

    public FlagDataTypeConfig getInheritedDataTypeConfig() {
        fmc.validate(); // causes datatype configs, like foo, to inherit
                        // properties from default Config
        // typically foo is the first
        FlagDataTypeConfig dataTypeConfig = fmc.getFlagConfigs().get(0);
        // verify set up
        assertEquals(10L, dataTypeConfig.getReducers());
        assertFalse(dataTypeConfig.isLifo());
        return dataTypeConfig;
    }

    public long getMinFolderTime() {
        return this.timeTracker.minFolderTime;
    }

    public long getMaxFolderTime() {
        return this.timeTracker.maxFolderTime;
    }

    public long getMinLastModified() {
        return this.timeTracker.minLastModified;
    }

    public long getMaxLastModified() {
        return this.timeTracker.maxLastModified;
    }
    public Collection<Long> getLastModifiedTimes() {
        return timeTracker.fileLastModifiedTimes;
    }

    public Collection<String> getNamesOfCreatedFiles() {
        return new ArrayList<>(flagFileInputStructure.getNamesOfCreatedFiles());
    }

    public void deleteTestDirectories() throws IOException {
        for (String directory : new String[] {fmc.getBaseHDFSDir(), fmc.getFlagFileDirectory(), fmc.getFlagMetricsDirectory()}) {
            File f = new File(directory);
            if (f.exists()) {
                // commons io has recursive delete.
                FileUtils.deleteDirectory(f);
            }
        }
    }

    public void createAdditionalTestFiles() throws IOException {
        this.flagFileInputStructure.createAdditionalTestFiles();
    }

    public void createTrackedDirsForInputFile(InputFile inputFile) throws IOException {
        final Path[] dirs = {inputFile.getFlagged(), inputFile.getFlagging(), inputFile.getLoaded()};
        for (final Path dir : dirs) {
            final Path p = dir.getParent();
            if (!fs.mkdirs(p)) {
                throw new IllegalStateException("unable to create tracked directory (" + dir.getParent() + ")");
            }
        }
    }
}