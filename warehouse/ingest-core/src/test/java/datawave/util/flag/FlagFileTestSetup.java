package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.StringUtils;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;

/**
 * Simplifies the loading of a sample flag maker configuration file as well as the generation of test files that align with the configuration.
 */
public class FlagFileTestSetup {
    private static final Logger LOG = LoggerFactory.getLogger(FlagFileTestSetup.class);

    private static final String TEST_CONFIG = "target/test-classes/TestFlagMakerConfig.xml";
    private static final String DATE_PATH = "2013/01";
    private static long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

    private boolean usePredicableInputFilenames = false;
    private int filesPerDay = 1;
    private int numDays = 1;
    private int numFoldersCreated = 0;
    private String subDirectoryName;

    private final Collection<String> createdFiles = new ArrayList<>();

    public FlagMakerConfig fmc;
    public final FileSystem fs;
    public final Collection<Long> lastModifiedTimes = new ArrayList<>();

    private long minLastModified = Long.MAX_VALUE;
    private long maxLastModified = Long.MIN_VALUE;
    private long minFolderTime = Long.MAX_VALUE;
    private long maxFolderTime = Long.MIN_VALUE;
    private String suffix = null;

    public FlagFileTestSetup() {
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
            createDirectory(fmc.getBaseHDFSDir(), "base HDFS");
            LOG.info("Set base HDFS directory to " + this.fmc.getBaseHDFSDir());

            if (this.fmc.getFlagFileDirectory().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified test directory name");
            }
            this.fmc.setFlagFileDirectory(this.fmc.getFlagFileDirectory().replace("target", "target/" + subDirectoryName));
            LOG.info("Set flag file directory to " + this.fmc.getFlagFileDirectory());
            createDirectory(fmc.getFlagFileDirectory(), "flag");

            if (this.fmc.getFlagMetricsDirectory().contains(this.subDirectoryName)) {
                throw new RuntimeException("Already modified metrics directory name");
            }
            this.fmc.setFlagMetricsDirectory(this.fmc.getFlagMetricsDirectory().replace("target", "target/" + subDirectoryName));
            LOG.info("Set flag file directory to " + this.fmc.getFlagMetricsDirectory());
            createDirectory(fmc.getFlagMetricsDirectory(), "flag");
        }
        return this;
    }

    public FlagFileTestSetup withConfig(FlagMakerConfig fmc) throws IOException {
        this.fmc = fmc;

        if (null != subDirectoryName) {
            withTestNameForDirectories(subDirectoryName);
        } else {
            createDirectory(fmc.getBaseHDFSDir(), "base HDFS");
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

    public void createTestFiles() throws IOException {
        emptyDirectories();
        createAdditionalTestFiles();
    }

    /**
     * Creates test files without first emptying the directories. Expected to be called after createTestFiles()
     */
    public void createAdditionalTestFiles() throws IOException {
        if (numDays < 1 || numDays > 9)
            throw new IllegalArgumentException("days argument must be [1-9]. Incorrect value was: " + numDays);
        // use relative paths for testing
        ArrayList<File> inputDirs = new ArrayList<>(10);
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            for (String listOfFolderValues : fc.getFolders()) {
                for (String folderName : StringUtils.split(listOfFolderValues, ',')) {
                    folderName = folderName.trim();
                    if (!folderName.startsWith(fmc.getBaseHDFSDir())) {
                        // we do this conditionally because once the FileMaker
                        // is created and the setup call is made, this
                        // is already done.
                        folderName = fmc.getBaseHDFSDir() + File.separator + folderName;
                    }
                    inputDirs.add(new File(folderName));
                    numFoldersCreated++;
                }
            }
        }
        for (File inputDir : inputDirs) {
            for (int dayNumber = 1; dayNumber <= numDays; dayNumber++) {
                File directory = createDirectory(inputDir, dayNumber);
                long time = getTimestampForFilesInDirectory(dayNumber);
                for (int j = 0; j < filesPerDay; j++) {
                    createFile(directory, time);
                }
            }
        }
        LOG.info("Created " + numFoldersCreated + " folders and " + createdFiles.size() + " files (cumulative).");
    }

    private File createDirectory(File baseDirectory, int dayNumber) {
        File directory = new File(baseDirectory.getAbsolutePath() + File.separator + DATE_PATH + File.separator + "0" + dayNumber);
        directory.deleteOnExit();

        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Unable to make dirs for " + directory.toString());
            }
        }
        return directory;
    }

    // returns a timestamp corresponding to ten days later than the directory
    private long getTimestampForFilesInDirectory(int dayNumber) {
        long folderTimeInMillis = LocalDate.of(2013, 1, dayNumber).toEpochDay() * ONE_DAY_IN_MILLIS;

        this.minFolderTime = Math.min(this.minFolderTime, folderTimeInMillis);
        this.maxFolderTime = Math.max(this.maxFolderTime, folderTimeInMillis);

        // return a day that is 10 days past the folder date
        return folderTimeInMillis + (10 * ONE_DAY_IN_MILLIS);
    }

    private void createFile(File directory, long time) throws IOException {
        String fileName = createFileName();
        String absolutePath = directory.getAbsolutePath();
        File testFile = new File(absolutePath + File.separator + fileName);
        testFile.deleteOnExit();

        if (testFile.exists()) {
            if (!testFile.delete()) {
                throw new RuntimeException("Unable to delete " + testFile.toString());
            }
        }
        try (FileOutputStream fos = new FileOutputStream(testFile)) {
            fos.write(("" + System.currentTimeMillis()).getBytes());
        }
        long lastModified = time + (createdFiles.size() * 1000L);

        createdFiles.add(fileName);
        this.lastModifiedTimes.add(lastModified);
        this.minLastModified = Math.min(lastModified, this.minLastModified);
        this.maxLastModified = Math.max(lastModified, this.maxLastModified);

        if (!testFile.setLastModified(lastModified)) {
            throw new RuntimeException("Unable to setLastModified for " + testFile.toString());
        }
    }

    private String createFileName() {
        String result;
        if (usePredicableInputFilenames) {
            result = new UUID(0, createdFiles.size()).toString();
        } else {
            result = UUID.randomUUID().toString();
        }
        if (null != suffix) {
            result = result + suffix;
        }
        return result;
    }

    private void emptyDirectories() throws IOException {
        createDirectory(fmc.getBaseHDFSDir(), "base HDFS");
        createDirectory(fmc.getFlagFileDirectory(), "flag file directory");
        createDirectory(fmc.getFlagMetricsDirectory(), "metrics directory");
    }

    private void createDirectory(String directory, String description) throws IOException {
        File f = new File(directory);
        if (f.exists()) {
            // commons io has recursive delete.
            FileUtils.deleteDirectory(f);
        }
        if (!f.mkdirs()) {
            throw new IOException("unable to create " + description + " directory (" + f.getAbsolutePath() + ")");
        }
    }

    public FlagDataTypeConfig getInheritedDataTypeConfig() {
        fmc.validate(); // causes datatype configs, like foo, to inherit
                        // properties from default Config
        FlagDataTypeConfig dataTypeConfig = fmc.getFlagConfigs().get(0); // typically
                                                                         // foo
                                                                         // is
                                                                         // the
                                                                         // first
        // verify set up
        assertEquals(10L, dataTypeConfig.getReducers());
        assertFalse(dataTypeConfig.isLifo());
        return dataTypeConfig;
    }

    public long getMinFolderTime() {
        return this.minFolderTime;
    }

    public long getMaxFolderTime() {
        return this.maxFolderTime;
    }

    public long getMinLastModified() {
        return this.minLastModified;
    }

    public long getMaxLastModified() {
        return this.maxLastModified;
    }

    public Collection<String> getNamesOfCreatedFiles() {
        return new ArrayList<>(createdFiles);
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
}
