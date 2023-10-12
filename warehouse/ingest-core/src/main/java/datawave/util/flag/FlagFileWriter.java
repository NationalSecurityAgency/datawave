package datawave.util.flag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;

//@formatter:off
/**
 * Create the flag file. This is done in several steps to ensure we can easily
 * recover if the process is killed somewhere in-between.
 *
 * <ul>
 * <li>move the files to the flagging directory for those that do not already
 * exist in flagging, flagged, or loaded</li>
 * <li>create the flag.generating file for those files we were able to move, and
 * do not exist elsewhere</li>
 * <li>set the timestamp of the flag file to that of the most recent file</li>
 * <li>move the flagging files to the flagged directory</li>
 * <li>move the generating file to the final flag file form</li>
 * </ul>
 *
 * Using these steps, a cleanup of an abnormal termination is as follows:
 * <ul>
 * <li>move all flagging files to the base directory</li>
 * <li>for all flag.generating files, move the flagged files to the base
 * directory</li>
 * <li>remove the flag.generating files.</li>
 * </ul>
 */
// @formatter:on
public class FlagFileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(FlagFileWriter.class);
    static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#0.00");

    private final FlagMakerConfig flagMakerConfig;
    private final FileSystem fs;
    private final ExecutorService fileMoveExecutor;
    // Directory cache serves as a placeholder for the directories in HDFS that
    // were created, reducing the number of RPC calls to the NameNode
    private final Cache<Path,Path> directoryCache;
    private final FlagFileContentCreator flagFileContentCreator;
    private FlagMetrics metrics;

    public FlagFileWriter(FlagMakerConfig flagMakerConfig) throws IOException {
        this.flagMakerConfig = flagMakerConfig;
        this.flagFileContentCreator = new FlagFileContentCreator(this.flagMakerConfig);
        this.fs = FlagMakerConfigUtility.getHadoopFS(flagMakerConfig);
        this.fileMoveExecutor = Executors.newFixedThreadPool(flagMakerConfig.getMaxHdfsThreads());
        this.directoryCache = buildDirectoryCache();
    }

    /**
     * @param fc
     *            flag configuration
     * @param inputFiles
     *            input files to write to flag file
     * @throws IOException
     *             error trying to write flag file or move files
     */
    // todo - sort order not maintained
    void writeFlagFile(final FlagDataTypeConfig fc, Collection<InputFile> inputFiles) throws IOException {
        File flagFile = null;
        long now = System.currentTimeMillis();
        this.metrics = createFlagMetrics(fs, fc);

        // futures holds tasks in case of a failure scenario, so that they can
        // be completed
        List<Future<InputFile>> futures = Lists.newArrayList();

        try {
            Collection<InputFile> flaggingFiles = moveFilesToFlagging(inputFiles, futures);
            assert futures.size() == 0 : futures.size();

            // generate flag file
            String baseName = createFlagFileBaseName(fc, now, inputFiles);
            flagFile = write(flaggingFiles, fc, baseName);
            setFlagFileTimestampToLatestInputFileTimestamp(flagFile, flaggingFiles);

            // update metrics
            updateMetricsWithInputFileTimestamps(fc, metrics, flaggingFiles);

            // finalize flag file
            moveFilesToFlagged(fc, metrics, flaggingFiles, futures);
            assert futures.size() == 0 : futures.size();

            flagFile = removeGeneratingExtension(flagFile, baseName);

            resetTimeoutInterval(fc, now);

            writeMetricsFile(fc, metrics, baseName);
        } catch (AssertionError | Exception ex) {
            // todo see other todo in moveFilesBack.
            // note that cleanupMovesFilesFromFlaggedToInputDir,
            // cleanupMovesFilesFromFlaggingToInputDir gets here as does
            // cleanupOccursInProperOrder,
            // cleanupRemovesGeneratingFlagFiles
            LOG.error("Unable to complete flag file ", ex);
            moveFilesBack(inputFiles, futures);
            assert futures.size() == 0 : futures.size();

            removeFlagFile(flagFile);
            throw ex;
        }
    }

    @VisibleForTesting
    Collection<InputFile> moveFilesToFlagging(Collection<InputFile> inputFiles, List<Future<InputFile>> futures) throws IOException {
        return moveFiles(inputFiles, futures, this::createMoverToFlagging, "flagging");
    }

    @VisibleForTesting
    void moveFilesToFlagged(FlagDataTypeConfig fc, FlagMetrics metrics, Collection<InputFile> flaggingFiles, List<Future<InputFile>> futures)
                    throws IOException {
        Collection<InputFile> flaggedFiles = moveFiles(flaggingFiles, futures, this::createMoverToFlagged, "flaggedFiles");

        if (fc.isCollectMetrics()) {
            for (InputFile entry : flaggedFiles) {
                metrics.addFlaggedTime(entry);
            }
        }
    }

    @VisibleForTesting
    Collection<InputFile> moveFiles(Collection<InputFile> inputFiles, List<Future<InputFile>> futures, Function<InputFile,SimpleMover> moverFactory,
                    String label) throws IOException {
        assert futures.size() == 0; // futures will be populated with
        for (final InputFile inputFile : inputFiles) {
            // Creates directories and moves to flagging
            final Callable<InputFile> mover = moverFactory.apply(inputFile);
            final Future<InputFile> exec = fileMoveExecutor.submit(mover);
            futures.add(exec);
        }

        Collection<InputFile> movedFiles = new HashSet<>();
        // if any files were not moved, then throw an Exception
        if (!waitForMoves(movedFiles, futures)) {
            String message = "Unable to move files to " + label + ". Investigate";
            LOG.warn(message);
            throw new IOException(message);
        }
        return movedFiles;
    }

    private SimpleMover createMoverToFlagging(InputFile inputFile) {
        return new FlagEntryMover(directoryCache, fs, inputFile);
    }

    @VisibleForTesting
    SimpleMover createMoverToFlagged(InputFile inputFile) {
        return new SimpleMover(directoryCache, inputFile, InputFile.TrackedDir.FLAGGED_DIR, fs);
    }

    private void resetTimeoutInterval(FlagDataTypeConfig fc, long now) {
        fc.setLast(now + fc.getTimeoutMilliSecs());
    }

    void removeFlagFile(File flagFile) {
        if (flagFile != null) {
            if (!flagFile.delete()) {
                LOG.warn("unable to delete flag file ({})", flagFile.getAbsolutePath());
            }
        }
    }

    private void updateMetricsWithInputFileTimestamps(FlagDataTypeConfig fc, FlagMetrics metrics, Collection<InputFile> flagging) {
        if (fc.isCollectMetrics()) {
            for (InputFile entry : flagging) {
                metrics.addInputFileTimestamp(entry);
            }
        }
    }

    private void setFlagFileTimestampToLatestInputFileTimestamp(File flagFile, Collection<InputFile> flagging) {
        final AtomicLong latestTime = new AtomicLong(-1);

        for (InputFile entry : flagging) {
            latestTime.set(Math.max(entry.getTimestamp(), latestTime.get()));
        }

        // now set the modification time of the flag file
        if (this.flagMakerConfig.isSetFlagFileTimestamp()) {
            if (!flagFile.setLastModified(latestTime.get())) {
                LOG.warn("unable to set last modified time for flag file ({})", flagFile.getAbsolutePath());
            }
        }
    }

    // Returns a String containing the directory and name of the flag file, just
    // without the file name extension

    private String createFlagFileBaseName(FlagDataTypeConfig fc, long now, Collection<InputFile> inputFiles) {
        String firstInputFileName = inputFiles.iterator().next().getCurrentDir().getName();
        String nowInSeconds = DECIMAL_FORMAT.format(now / 1000);
        int numberOfInputFiles = inputFiles.size();
        String flagFileNameWithoutExtension = nowInSeconds + "_" + fc.getIngestPool() + "_" + fc.getDataName() + "_" + firstInputFileName + "+"
                        + numberOfInputFiles;

        String flagFileDirectory = this.flagMakerConfig.getFlagFileDirectory();

        return flagFileDirectory + File.separator + flagFileNameWithoutExtension;
    }

    private void writeMetricsFile(FlagDataTypeConfig fc, FlagMetrics metrics, String baseName) {
        if (fc.isCollectMetrics()) {
            try {
                metrics.writeMetrics(this.flagMakerConfig.getFlagMetricsDirectory(), new Path(baseName).getName());
            } catch (Exception ex) {
                LOG.warn("Non-fatal Exception encountered when writing metrics.", ex);
            }
        }
    }

    @VisibleForTesting
    File removeGeneratingExtension(File flagFile, String baseName) throws IOException {
        File f2 = new File(baseName + ".flag");
        if (!flagFile.renameTo(f2)) {
            throw new IOException("Failed to rename" + flagFile.toString() + " to " + f2);
        }
        flagFile = f2;
        return flagFile;
    }

    @VisibleForTesting
    FlagMetrics createFlagMetrics(FileSystem fs, FlagDataTypeConfig fc) {
        return new FlagMetrics(this.fs, fc.isCollectMetrics());
    }

    /**
     * Shuts down resources
     */
    public void close() {
        fileMoveExecutor.shutdown();
    }

    /**
     * Creates the flag file using all of the valid ingest files.
     *
     * @param flagging
     *            ingest files
     * @param fc
     *            data type for ingest
     * @param baseName
     *            base name for flag file
     * @return handle for flag file
     * @throws IOException
     *             error creating flag file
     */
    // todo - sort order maintained
    @VisibleForTesting
    File write(Collection<InputFile> flagging, FlagDataTypeConfig fc, String baseName) throws IOException {
        File flagGeneratingFile = createFlagGeneratingFile(flagging, fc, baseName);
        SortedSet<InputFile> sortedFlagging = new TreeSet<>(fc.isLifo() ? InputFile.LIFO : InputFile.FIFO); // todo - verify default
        sortedFlagging.addAll(flagging);
        try (FileOutputStream flagOS = new FileOutputStream(flagGeneratingFile)) {
            this.flagFileContentCreator.writeFlagFileContents(flagOS, sortedFlagging, fc);
            this.writeFileNamesToMetrics(sortedFlagging);
        }
        return flagGeneratingFile;
    }

    private void writeFileNamesToMetrics(Collection<InputFile> inputFiles) {
        if (metrics != null) {
            boolean first = true;
            for (InputFile inFile : inputFiles) {
                // todo - add a test where this fails, then consider refactoring
                if (first) {
                    first = false;
                } else {
                    metrics.addInputFileTimestamp(inFile);
                }
            }
        }
    }

    private File createFlagGeneratingFile(Collection<InputFile> flagging, FlagDataTypeConfig fc, String baseName) throws IOException {
        LOG.debug("Creating flag file {}.flag for data type {} containing {} files", baseName, fc.getDataName(), flagging.size());
        File f = new File(baseName + ".flag.generating");
        if (!f.createNewFile()) {
            throw new IOException("Unable to create flag file " + f.getName());
        }
        return f;
    }

    /**
     *
     * @param files
     *            InputFile flag file entries to move back to the input path directory
     * @param moveOperations
     *            Future objects, each attempting a move operation
     * @throws IOException
     *             failure in moving files
     */
    @VisibleForTesting
    void moveFilesBack(Collection<InputFile> files, List<Future<InputFile>> moveOperations) throws IOException {
        if (files.isEmpty()) {
            LOG.info("No files to move back");
            return;
        }

        if (moveOperations.size() > 0) {
            retryMoveOperations(moveOperations);
        }

        // Create a move operation (Future) for each flag entry, and add the
        // operation to moveOperations
        for (InputFile flagEntry : files) {
            final SimpleMover mover = new SimpleMover(directoryCache, flagEntry, InputFile.TrackedDir.PATH_DIR, fs);
            final Future<InputFile> exec = fileMoveExecutor.submit(mover);
            moveOperations.add(exec);
        }
        // Determine which files were moved
        HashSet<InputFile> moved = new HashSet<>();

        try {
            waitForMoves(moved, moveOperations);

            if (moved.size() != files.size()) {
                StringBuilder sb = new StringBuilder();
                for (InputFile entry : files) {
                    if (!entry.getPath().equals(entry.getCurrentDir())) {
                        sb.append("\n").append(entry.getCurrentDir().toString());
                    }
                }
                // todo - add one more retry if we know the name of the file?
                LOG.error("An error occurred while attempting to move files. The following files were orphaned:{}", sb.toString());
            }
        } catch (Exception ex) {
            LOG.error("Failed while waiting for files to be moved back to input directory", ex);
            // proceed without rethrowing exception because remaining clean up
            // is needed
        }
    }

    private void retryMoveOperations(List<Future<InputFile>> moveOperations) {
        LOG.error("Unexpected state: moveOperations contains {} entries.", moveOperations.size());
        int numFailedRetries = 0;
        for (Future<InputFile> moveOperation : moveOperations) {
            try {
                InputFile inputFile = moveOperation.get(1, TimeUnit.SECONDS);
                if (inputFile != null) {
                    LOG.info("Successfully retried move operation for {}", inputFile.toString());
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                numFailedRetries++;
            }
            LOG.error("Number of move operations that failed upon retry: {}", numFailedRetries);
        }
        moveOperations.clear();
    }

    /**
     * Populates movedInputFiles parameter with InputFile objects provided by moveOperations Future objects.
     *
     * @param movedInputFiles
     *            collection to be populated by this method with moved InputFiles
     * @param moveOperations
     *            list of move operations (Future objects) that will return InputFiles
     * @return true if and only if movedInputFiles is not empty and moveOperations did not experience an exception
     * @throws IOException
     *             if any of the moveOperations saw InterruptedException or ExecutionException
     */
    @VisibleForTesting
    boolean waitForMoves(Collection<InputFile> movedInputFiles, List<Future<InputFile>> moveOperations) throws IOException {
        IOException ioException = null;

        for (Future<InputFile> moveOperation : moveOperations) {
            try {
                InputFile inputFile = moveOperation.get();
                if (inputFile != null && inputFile.isMoved()) {
                    movedInputFiles.add(inputFile);
                }
                // test - try adding CancellationException case
                // todo - if inputFile is null or isMoved is false, shouldn't
                // that be considered a failure scenario and treated
                // accordingly?
            } catch (CancellationException | InterruptedException | ExecutionException ex) { // todo - change to all
                                                                                             // exceptions?
                ioException = new IOException("Failure during move", ex.getCause());
            }
        }
        // todo - why clear this
        moveOperations.clear();

        if (ioException != null) {
            throw ioException;
        }
        return !movedInputFiles.isEmpty();
    }

    private Cache<Path,Path> buildDirectoryCache() {
        // build the cache per the default configuration.
        // @formatter:off
		return CacheBuilder
				.newBuilder()
				.maximumSize(this.flagMakerConfig.getDirectoryCacheSize())
				.expireAfterWrite(
						this.flagMakerConfig.getDirectoryCacheTimeout(),
						TimeUnit.MILLISECONDS)
				.concurrencyLevel(this.flagMakerConfig.getMaxHdfsThreads())
				.build();
		// @formatter:on
    }
}
