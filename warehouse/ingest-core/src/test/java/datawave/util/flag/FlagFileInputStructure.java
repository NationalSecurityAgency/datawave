package datawave.util.flag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import datawave.util.flag.config.FlagMakerConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.StringUtils;
import datawave.util.flag.config.FlagDataTypeConfig;

/**
 * Assists with creating input files and input directory structure for flag maker tests
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

    protected void createDirectory(String directory) throws IOException {
        File file = deleteDirectoryIfExists(directory);
        if (!file.mkdirs()) {
            throw new IOException("unable to create " + directory + " directory (" + file.getAbsolutePath() + ")");
        }
    }

    private File createDirectory(File baseDirectory, int dayNumber) {
        String dayOfMonth = Strings.padStart(Integer.toString(dayNumber), 2, '0');
        File directory = new File(baseDirectory.getAbsolutePath() + File.separator + DATE_PATH + File.separator + dayOfMonth);
        directory.deleteOnExit();

        if (!directory.exists() && !directory.mkdirs()) {
            throw new RuntimeException("Unable to make dirs for " + directory.toString());
        }
        return directory;
    }

    private File deleteDirectoryIfExists(String directory) throws IOException {
        File file = new File(directory);
        if (file.exists()) {
            // commons io has recursive delete.
            FileUtils.deleteDirectory(file);
        }
        return file;
    }

    protected void createEmptyDirectories() throws IOException {
        for (String directory : getTestDirectories()) {
            createDirectory(directory);
        }
    }

    public void deleteTestDirectories() throws IOException {
        for (String directory : getTestDirectories()) {
            deleteDirectoryIfExists(directory);
        }
    }

    private String[] getTestDirectories() {
        FlagMakerConfig flagMakerConfig = this.flagFileTestSetup.getFlagMakerConfig();
        // @formatter:off
        return new String[] {
                flagMakerConfig.getBaseHDFSDir(),
                flagMakerConfig.getFlagFileDirectory(),
                flagMakerConfig.getFlagMetricsDirectory()
        };
        // @formatter:on
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

        LOG.info("Created {} folders and {} files (cumulative).", numFoldersCreated, createdFiles.size());
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
                String baseHDFSDir = flagFileTestSetup.getFlagMakerConfig().getBaseHDFSDir();
                if (!folderName.startsWith(baseHDFSDir)) {
                    folderName = baseHDFSDir + File.separator + folderName;
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
}
