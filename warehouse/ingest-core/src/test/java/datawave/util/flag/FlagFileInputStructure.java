package datawave.util.flag;

import datawave.util.StringUtils;
import datawave.util.flag.config.FlagDataTypeConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Contains code to assist with creating input files and input directory structure for flag maker tests
 */
public class FlagFileInputStructure {
    private static final Logger LOG = LoggerFactory.getLogger(FlagFileInputStructure.class);

    private static final String YEAR = "2013";
    private static final String MONTH = "01";
    private static final String DATE_PATH = YEAR + "/" + MONTH;

    private static final long TIME_OFFSET_OF_FILES_IN_FOLDER = TimeUnit.DAYS.toMillis(10);

    private final FlagFileTestSetup flagFileTestSetup;
    private final FlagMakerTimestampTracker timeTracker;

    private final Collection<String> createdFiles;
    private int numFoldersCreated = 0;

    public FlagFileInputStructure(FlagFileTestSetup flagFileTestSetup, FlagMakerTimestampTracker timeTracker) {
        this.flagFileTestSetup = flagFileTestSetup;
        this.createdFiles = new ArrayList<>();
        this.timeTracker = timeTracker;
    }

    protected void createDirectory(String directory, String description) throws IOException {
        File f = new File(directory);
        if (f.exists()) {
            // commons io has recursive delete.
            FileUtils.deleteDirectory(f);
        }
        if (!f.mkdirs()) {
            throw new IOException("unable to create " + description + " directory (" + f.getAbsolutePath() + ")");
        }
    }

    protected void emptyDirectories() throws IOException {
        createDirectory(flagFileTestSetup.getFlagMakerConfig().getBaseHDFSDir(), "base HDFS");
        createDirectory(flagFileTestSetup.getFlagMakerConfig().getFlagFileDirectory(), "flag file directory");
        createDirectory(flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory(), "metrics directory");
    }

    /**
     * Creates test files without first emptying the directories. Expected to be called after createTestFiles()
     */
    protected void createAdditionalTestFiles() throws IOException {
        if (flagFileTestSetup.getNumDays() < 1 || flagFileTestSetup.getNumDays() > 9) {
            throw new IllegalArgumentException("days argument must be [1-9]. Incorrect value was: " + flagFileTestSetup.getNumDays());
        }

        ArrayList<File> inputDirs = new ArrayList<>(10);
        for (FlagDataTypeConfig flagDataTypeConfig : flagFileTestSetup.getFlagMakerConfig().getFlagConfigs()) {
            createInputDirectoriesForDataTypeConfig(inputDirs, flagDataTypeConfig);
        }

        numFoldersCreated += inputDirs.size();

        for (File inputDir : inputDirs) {
            createDirectoriesAndFilesForSpecifiedDays(inputDir, flagFileTestSetup.getNumDays(), flagFileTestSetup.getNumFilesPerDay());
        }

        LOG.info("Created " + numFoldersCreated + " folders and " + createdFiles.size() + " files (cumulative).");
    }

    protected Collection<String> getNamesOfCreatedFiles() {
        return this.createdFiles;
    }

    private void createDirectoriesAndFilesForSpecifiedDays(File inputDir, int numDays, int filesPerDay) throws IOException {
        for (int dayNumber = 1; dayNumber <= numDays; dayNumber++) {
            File directory = createDirectory(inputDir, dayNumber);
            long time = getTimestampForFilesInDirectory(dayNumber);
            createNFilesInDirectory(directory, time, filesPerDay);
        }
    }

    private void createNFilesInDirectory(File directory, long fileTimestamp, int numberFilesToCreate) throws IOException {
        for (int j = 0; j < numberFilesToCreate; j++) {
            createFile(directory, fileTimestamp);
        }
    }

    private void createInputDirectoriesForDataTypeConfig(ArrayList<File> inputDirs, FlagDataTypeConfig dataTypeConfig) {
        for (String listOfFolderValues : dataTypeConfig.getFolders()) {
            for (String folderName : StringUtils.split(listOfFolderValues, ',')) {
                folderName = folderName.trim();
                if (!folderName.startsWith(flagFileTestSetup.getFlagMakerConfig().getBaseHDFSDir())) {
                    // we do this conditionally because once the FileMaker
                    // is created and the setup call is made, this
                    // is already done.
                    folderName = flagFileTestSetup.getFlagMakerConfig().getBaseHDFSDir() + File.separator + folderName;
                }
                inputDirs.add(new File(folderName));
            }
        }
    }

    /**
     * @param dayNumber day of the month
     * @return timestamp to use for files = date of folder, e.g. 2013/01/09, plus an offset
     */
    private long getTimestampForFilesInDirectory(int dayNumber) {
        long timestampOfDateInFolderPath = TimeUnit.DAYS.toMillis(LocalDate.of(Integer.parseInt(YEAR), Integer.parseInt(MONTH), dayNumber).toEpochDay());
        timeTracker.reportDateForFolder(timestampOfDateInFolderPath);

        return timestampOfDateInFolderPath + TIME_OFFSET_OF_FILES_IN_FOLDER;
    }

    private void createFile(File directory, long folderTimestamp) throws IOException {
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
        long lastModified = folderTimestamp + (createdFiles.size() * 1000L);

        createdFiles.add(fileName);
        timeTracker.reportFileLastModified(lastModified);

        if (!testFile.setLastModified(lastModified)) {
            throw new RuntimeException("Unable to setLastModified for " + testFile.toString());
        }
    }

    private String createFileName() {
        String result;
        if (flagFileTestSetup.arePredicableInputFilenames()) {
            result = new UUID(0, createdFiles.size()).toString();
        } else {
            result = UUID.randomUUID().toString();
        }
        if (null != flagFileTestSetup.getTestFileNameSuffix()) {
            result = result + flagFileTestSetup.getTestFileNameSuffix();
        }
        return result;
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

    public void deleteTestDirectories() throws IOException {
        String[] directories = {
                this.flagFileTestSetup.getFlagMakerConfig().getBaseHDFSDir(),
                this.flagFileTestSetup.getFlagMakerConfig().getFlagFileDirectory(),
                this.flagFileTestSetup.getFlagMakerConfig().getFlagMetricsDirectory()
        };

        for (String directory : directories) {
            File f = new File(directory);
            if (f.exists()) {
                // commons io has recursive delete.
                FileUtils.deleteDirectory(f);
            }
        }
    }
}