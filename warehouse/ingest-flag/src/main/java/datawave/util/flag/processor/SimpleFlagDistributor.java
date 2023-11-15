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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import datawave.util.FilteringIterator;
import datawave.util.FullPathGlobFilter;
import datawave.util.flag.InputFile;
import datawave.util.flag.InputFileCreatingIterator;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;

/**
 * Uses provided inputFileSource to retrieve up to FlagDataTypeConfig.maxFlags input files at a time. No groupings, just returns files that are pending in no
 * specific order.
 *
 * Expects loadFiles to be called prior to hasNext and next.
 */
public class SimpleFlagDistributor implements FlagDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFlagDistributor.class);

    private final FlagMakerConfig flagMakerConfig;
    private FileSystem fileSystem;

    private Set<InputFile> inputFileBuffer;
    private FlagDataTypeConfig flagDataTypeConfig;
    private List<Stream<InputFile>> inputFileStreams = null;
    private Iterator<InputFile> inputFileSource;

    public SimpleFlagDistributor(FlagMakerConfig flagMakerConfig) throws IOException {
        this.flagMakerConfig = flagMakerConfig;
        this.fileSystem = FlagMakerConfigUtility.getHadoopFS(this.flagMakerConfig);
    }

    @Override
    public boolean hasNext(boolean mustHaveMax) {
        assertLoadFilesWasCalled();

        if (isBufferEmpty() && isSourceEmpty()) {
            return false;
        }

        fillBuffer();

        if (mustHaveMax) {
            LOG.trace("mustHaveMax = true, buffer.size() = {}, flagDataTypeConfig.getMaxFlags() = {}", inputFileBuffer.size(),
                            flagDataTypeConfig.getMaxFlags());
            return inputFileBuffer.size() >= flagDataTypeConfig.getMaxFlags();
        } else {
            LOG.trace("mustHaveMax = false, buffer size = {}", (inputFileBuffer == null ? "null" : inputFileBuffer.size()));
            return !isBufferEmpty();
        }
    }

    @Override
    public Collection<InputFile> next(SizeValidator validator) {
        assertLoadFilesWasCalled();

        fillBuffer();

        int size = inputFileBuffer.size();
        if (size == 0) {
            return Collections.emptySet();
        }
        // Even though hasNext takes a parameter for mustHaveMax, next does not have that parameter
        // next assumes that mustHaveMax is false
        if (size < flagDataTypeConfig.getMaxFlags()) {
            return returnEntireBuffer();
        } else {
            return returnBufferUpToLimits(validator);
        }
    }

    private Collection<InputFile> returnBufferUpToLimits(SizeValidator validator) {
        Collection<InputFile> result = new HashSet<>();

        int cumulativeBlocks = 0;
        Iterator<InputFile> bufferIterator = inputFileBuffer.iterator();

        // while we have more potential files, and we have potentially room to add one
        while (bufferIterator.hasNext() && (cumulativeBlocks < flagDataTypeConfig.getMaxFlags())) {
            InputFile inFile = bufferIterator.next();

            cumulativeBlocks += getEstimatedBlocksForFile(inFile);
            if (cumulativeBlocks > flagDataTypeConfig.getMaxFlags()) {
                // this input file would put flag file past its threshold
                return result;
            }

            // add to result
            result.add(inFile);

            // Remove inFile from the buffer as long as the result size is valid or one
            if (result.size() == 1) {
                bufferIterator.remove();
            } else if (validator.isValidSize(flagDataTypeConfig, result)) {
                bufferIterator.remove();
            } else {
                // if invalid size, undo the addition
                result.remove(inFile);
                return result;
            }
        }
        return result;
    }

    private int getEstimatedBlocksForFile(InputFile inFile) {
        int estimatedBlocksForFile = inFile.getMaps();
        if (estimatedBlocksForFile > flagDataTypeConfig.getMaxFlags()) {
            LOG.warn("Estimated map cumulativeBlocksNeeded ({}) for file exceeds maxFlags ({}). Consider increasing maxFlags to accommodate larger files, or split this file into smaller chunks. File: {}",
                            estimatedBlocksForFile, flagDataTypeConfig.getMaxFlags(), inFile.getFileName());
        }
        return estimatedBlocksForFile;
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
        this.inputFileBuffer = new TreeSet<>(this.flagDataTypeConfig.isLifo() ? InputFile.LIFO : InputFile.FIFO);
        this.inputFileStreams = new ArrayList<>();
        this.inputFileSource = null;
    }

    /**
     * @param folder
     *            folder to use for input files
     * @throws IOException
     *             when a problem is encountered while accessing filesystem
     */
    private void registerInputFileIterator(String folder) throws IOException {
        LOG.debug("Examining {}", folder);

        Path fullFolderPath = new Path(fileSystem.getWorkingDirectory(), new Path(folder));
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

        for (String filePattern : this.flagMakerConfig.getFilePatterns()) {
            String folderPattern = folder + "/" + filePattern;
            LOG.trace("loading files for {} in folder: {} matching pattern: {}", this.flagDataTypeConfig.getDataName(), folder, folderPattern);

            // Prepend the working directory to limit filtering to a single pattern match
            String patternExtendedToFullPath = new Path(fileSystem.getWorkingDirectory(), new Path(folderPattern)).toString();

            LOG.debug("Prepended working directory: {} and is now: {}", fileSystem.getWorkingDirectory(), patternExtendedToFullPath);
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
     *             when a problem is encountered while accessing filesystem
     */
    private Iterator<InputFile> getInputFileIterator(PathFilter filter, Path fullFolderPath, String relativeFolderName) throws IOException {
        if (!fileSystem.exists(fullFolderPath)) {
            LOG.debug("Does not exist: {}", fullFolderPath.toUri().toString());
            return null;
        }

        LOG.debug("Creating remote file iterator for {}", fullFolderPath);

        // recursively lists files (skips directories as per API)
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(fullFolderPath, true);
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

        return new InputFileCreatingIterator(filteringFileIterator, relativeFolderName, this.flagMakerConfig.getBaseHDFSDir(),
                        this.flagMakerConfig.isUseFolderTimestamp());
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
        return inputFileBuffer == null || inputFileBuffer.size() == 0;
    }

    private boolean isSourceEmpty() {
        return this.inputFileSource == null || !this.inputFileSource.hasNext();
    }

    /**
     * Fill up buffer with InputFiles, up to size of FlagDataTypeConfig.maxFlags or until the source has nothing left.
     */
    private void fillBuffer() {
        while (inputFileBuffer.size() < flagDataTypeConfig.getMaxFlags() && null != this.inputFileSource && this.inputFileSource.hasNext()) {
            InputFile next = this.inputFileSource.next();

            inputFileBuffer.add(next);
        }
    }

    private void assertLoadFilesWasCalled() {
        if (inputFileBuffer == null) {
            throw new IllegalStateException("loadFiles must be called");
        }
    }

    private Collection<InputFile> returnEntireBuffer() {
        Collection<InputFile> result = new HashSet<>(inputFileBuffer);
        inputFileBuffer.clear();
        return result;
    }

    /**
     * Remove the base directory from the folder, if it exists.
     */
    private String stripBaseHDFSDir(String folder) {
        String inputFolder = folder;

        if (inputFolder.startsWith(this.flagMakerConfig.getBaseHDFSDir())) {
            inputFolder = inputFolder.substring(this.flagMakerConfig.getBaseHDFSDir().length());
            if (inputFolder.startsWith(File.separator)) {
                inputFolder = inputFolder.substring(File.separator.length());
            }
        }
        return inputFolder;
    }

    @VisibleForTesting
    void setFileSystem(FileSystem fileSystemOverride) {
        this.fileSystem = fileSystemOverride;
    }
}
