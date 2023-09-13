package datawave.util.flag.processor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import datawave.util.FilteringIterator;
import datawave.util.FullPathGlobFilter;
import datawave.util.flag.InputFile;
import datawave.util.flag.InputFileCreatingIterator;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses provided inputFileSource to retrieve up to FlagDataTypeConfig.maxFlags input files at a time. No groupings, just returns files that are pending in no
 * specific order
 */
public class SimpleFlagDistributor implements FlagDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFlagDistributor.class);
    private FileSystem fs;

    private Set<InputFile> buffer;
    private FlagDataTypeConfig flagDataTypeConfig;
    private FlagMakerConfig fmc;
    private List<Stream<InputFile>> inputFileStreams = null;
    private Iterator<InputFile> inputFileSource;

    public SimpleFlagDistributor(FlagMakerConfig flagMakerConfig) throws IOException {
        this.fmc = flagMakerConfig;
        this.fs = FlagMakerConfigUtility.getHadoopFS(this.fmc);
    }

    @Override
    public boolean hasNext(boolean mustHaveMax) {
        assertLoadFilesWasCalled();

        if (isBufferEmpty() && isSourceEmpty()) {
            return false;
        }

        fillBuffer();

        return mustHaveMax ? buffer.size() >= flagDataTypeConfig.getMaxFlags() : !isBufferEmpty();
    }

    @Override
    public Collection<InputFile> next(SizeValidator validator) {
        assertLoadFilesWasCalled();

        fillBuffer();

        int size = buffer.size();
        if (size == 0) {
            return Collections.EMPTY_SET;
        }
        // Assumes that hasNext(false) returned true
        if (size < flagDataTypeConfig.getMaxFlags()) {
            return returnEntireBuffer();
        } else {
            Collection<InputFile> list = new HashSet<>();
            int cumulativeBlocksNeeded = 0;
            Iterator<InputFile> it = buffer.iterator();
            // while we have more potential files, and we have potentially room to add one
            while (it.hasNext() && (cumulativeBlocksNeeded < flagDataTypeConfig.getMaxFlags())) {
                InputFile inFile = it.next();

                int estimatedBlocksForFile = inFile.getMaps();
                if (estimatedBlocksForFile > flagDataTypeConfig.getMaxFlags()) {
                    LOG.warn("Estimated map cumulativeBlocksNeeded ({}) for file exceeds maxFlags ({}). Consider increasing maxFlags to accommodate larger files, or split this file into smaller chunks. File: {}",
                                    estimatedBlocksForFile, flagDataTypeConfig.getMaxFlags(), inFile.getFileName());
                }

                // update the cumulativeBlocksNeeded, and break out if this file would pass our threshold
                cumulativeBlocksNeeded += estimatedBlocksForFile;
                if (cumulativeBlocksNeeded > flagDataTypeConfig.getMaxFlags()) {
                    break;
                }

                // add it to the list
                list.add(inFile);

                // if valid (or only one file in the list), then continue normally
                if (validator.isValidSize(flagDataTypeConfig, list) || (list.size() == 1)) {
                    it.remove();
                } else {
                    // else remove the file back out and abort
                    list.remove(inFile);

                    break;
                }
            }
            return list;
        }
    }

    /**
     *
     * Looks for input files matching the specified data type config
     *
     * This is expected to be called repeatedly, similarly to how addInputFile was called repeatedly in the prior implementation. It'll likely use the same
     * configuration unless at some point the configuration is reread. It's expected to be called after the files from prior rounds have been distributed.
     *
     * @param flagDataTypeConfig
     *            configuration for a data type
     * @throws IOException
     *             failed to access file listings
     */
    @Override
    public void loadFiles(FlagDataTypeConfig flagDataTypeConfig) throws IOException {
        this.flagDataTypeConfig = flagDataTypeConfig;

        clearState();

        String dataName = this.flagDataTypeConfig.getDataName();
        LOG.debug("Checking for files for {}", dataName);

        List<String> dataTypeFolders = this.flagDataTypeConfig.getFolders();

        for (String folder : dataTypeFolders) {
            registerInputFileIterator(folder);
        }
    }

    /**
     * Ensures all remote iterators and buffered entries are cleared
     */
    private void clearState() {
        this.buffer = new TreeSet<>(this.flagDataTypeConfig.isLifo() ? InputFile.LIFO : InputFile.FIFO);
        this.inputFileStreams = new ArrayList<>();
        this.inputFileSource = null;
    }

    /**
     *
     * @param folder
     * @throws IOException
     */
    @VisibleForTesting
    void registerInputFileIterator(String folder) throws IOException {
        LOG.debug("Examining {}", folder);

        // todo - test what happens if folder already has the base dir
        Path fullFolderPath = new Path(fs.getWorkingDirectory(), new Path(folder));
        PathFilter filenamePatternFilter = new FullPathGlobFilter(gatherFilePatterns(folder));

        Iterator<InputFile> inputFileIterator = getInputFileIterator(filenamePatternFilter, fullFolderPath, stripBaseHDFSDir(folder));
        if (inputFileIterator != null) {
            LOG.info("Added source to FileDistributor");
            addSourceIterator(inputFileIterator);
        } else {
            LOG.info("Unable to find files for {}", folder);

        }
    }

    /**
     * Uses the configured work directory, folder parameter, and configured file patterns to construct a list of full path patterns that can be used to examine
     * HDFS.
     *
     * @param folder
     *            folder to examine
     * @return a list of full-path and filename patterns within HDFS
     */
    private List<String> gatherFilePatterns(String folder) {
        List<String> fullPathPatterns = new ArrayList<>();

        for (String filePattern : this.fmc.getFilePatterns()) {
            String folderPattern = folder + "/" + filePattern;
            LOG.trace("loading files for {} in folder: {} matching pattern: {}", this.flagDataTypeConfig.getDataName(), folder, folderPattern);

            // Prepend the working directory to limit filtering to a single pattern match
            String patternExtendedToFullPath = new Path(fs.getWorkingDirectory(), new Path(folderPattern)).toString();

            LOG.debug("Prepended working directory: {} and is now: {}", fs.getWorkingDirectory(), patternExtendedToFullPath);
            fullPathPatterns.add(patternExtendedToFullPath);
        }

        LOG.trace("Extended pattern(s) to {}", fullPathPatterns);

        return fullPathPatterns;
    }

    /**
     * Creates an InputFile iterator by wrapping a remote FileStatus iterator with a filename filtering iterator and a FileStatus=>InputFile converting iterator
     *
     * @param filter
     *            filter used to limit to matching filename patterns
     * @param fullFolderPath
     *            - full path, with schema and base HDFS dir
     * @param relativeFolderName
     *            - name of folder underneath base HDFS dir, to be used in flag file contents
     * @return an iterator of InputFile objects that match the patterns
     * @throws IOException
     */
    private Iterator<InputFile> getInputFileIterator(PathFilter filter, Path fullFolderPath, String relativeFolderName) throws IOException {
        if (!fs.exists(fullFolderPath)) {
            LOG.debug("Does not exist: {}", fullFolderPath.toUri().toString());
            return null;
        }

        LOG.debug("Creating remote file iterator for {}", fullFolderPath);

        // recursively lists files (skips directories as per API)
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(fullFolderPath, true);
        LOG.debug("Created remote file iterator for {}", fullFolderPath);

        if (!fileIterator.hasNext()) {
            LOG.debug("No files found under {}", fullFolderPath);
            return null;
        }

        FilteringIterator<LocatedFileStatus> filteringFileIterator = new FilteringIterator<>(fileIterator, filter);

        if (!filteringFileIterator.hasNext()) {
            LOG.debug("No matching file names found under {}", fullFolderPath);
            return null;
        }

        return new InputFileCreatingIterator(filteringFileIterator, relativeFolderName, this.fmc.getBaseHDFSDir(), this.fmc.isUseFolderTimestamp());
    }

    private void addSourceIterator(Iterator<InputFile> inputFileSourceIterator) {
        // convert iterator to a stream
        Iterable<InputFile> iterable = () -> inputFileSourceIterator;
        Stream<InputFile> stream = StreamSupport.stream(iterable.spliterator(), false);

        // save stream for later in case more sources will be added
        inputFileStreams.add(stream);

        // Create a new iterator from all the saved streams
        // Note: files may be retrieved, via next, before all of the streams are registered.
        Stream<InputFile> combinedStream = this.inputFileStreams.stream().flatMap(Function.identity());
        inputFileSource = combinedStream.iterator();
    }

    private boolean isBufferEmpty() {
        return buffer == null || buffer.size() == 0;
    }

    private boolean isSourceEmpty() {
        return this.inputFileSource == null || !this.inputFileSource.hasNext();
    }

    /**
     * Fill up buffer with InputFiles, up to size of FlagDataTypeConfig.maxFlags or until the source has nothing left.
     */
    private void fillBuffer() {
        // fill inputs to the desired size
        while (buffer.size() < flagDataTypeConfig.getMaxFlags() && null != this.inputFileSource && this.inputFileSource.hasNext()) {
            InputFile next = this.inputFileSource.next();

            buffer.add(next);
        }
    }

    private void assertLoadFilesWasCalled() {
        if (buffer == null) {
            throw new IllegalStateException("loadFiles must be called");
        }
    }

    private Collection<InputFile> returnEntireBuffer() {
        Collection<InputFile> result = new HashSet<>(buffer);
        buffer.clear();
        return result;
    }

    /**
     * Remove the base directory from the folder, if it exists.
     */
    private String stripBaseHDFSDir(String folder) {
        String inputFolder = folder;

        if (inputFolder.startsWith(this.fmc.getBaseHDFSDir())) {
            inputFolder = inputFolder.substring(this.fmc.getBaseHDFSDir().length());
            if (inputFolder.startsWith(File.separator)) {
                inputFolder = inputFolder.substring(File.separator.length());
            }
        }
        return inputFolder;
    }

    @VisibleForTesting
    void setFileSystem(FileSystem fileSystemOverride) {
        this.fs = fileSystemOverride;
    }
}
