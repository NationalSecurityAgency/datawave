package datawave.ingest.mapreduce.job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This extension of the FileOutputCommitter does a safety check to ensure no files were left around in the temporary directories before terminating. If there
 * were, then this will fail the job.
 */
public class SafeFileOutputCommitter extends FileOutputCommitter {
    private static final Logger LOG = LoggerFactory.getLogger(SafeFileOutputCommitter.class);
    
    /**
     * When LENIENT_MODE is set to false, the legacy behavior is maintained: if any files exist in the pending directory when cleanupJob is called, an exception
     * is thrown. When LENIENT_MODE is set to true, an exception is only thrown if one or more of the filenames that appear in the pending directory do not also
     * appear in the success directory. This mode should only be used if the filenames are consistent across multiple attempts of the same task and there is a
     * guarantee that a successful file is equivalent to, or better than, a pending file.
     */
    public static final String LENIENT_MODE = "mapreduce.safefileoutputcommitter.lenient.mode";
    
    private static final boolean DEFAULT_LENIENT_MODE = false;
    private final boolean lenientMode;
    
    // a boolean denoting whether we should check for an empty directory before cleaning it up
    private volatile boolean checkForEmptyDir = true;
    
    public SafeFileOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
        this.lenientMode = context.getConfiguration().getBoolean(LENIENT_MODE, DEFAULT_LENIENT_MODE);
    }
    
    public SafeFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
        this.lenientMode = context.getConfiguration().getBoolean(LENIENT_MODE, DEFAULT_LENIENT_MODE);
    }
    
    @Override
    public void abortJob(JobContext context, State state) throws IOException {
        // in this case the job is being killed, no need to check for empty dirs
        checkForEmptyDir = false;
        super.abortJob(context, state);
    }
    
    /**
     * Since the parent class hides this method, we need to recreate it here
     * 
     * @return the location of pending job attempts.
     */
    private Path getPendingJobAttemptsPath() {
        return new Path(super.getOutputPath(), PENDING_DIR_NAME);
    }
    
    /**
     * Cleanup the job. Note that this is deprecated in the super class but is still being used for this work. When the method has been removed from the super
     * class then this class will need to be reworked.
     * 
     * @param context
     *            The job context
     */
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        if (checkForEmptyDir && super.getOutputPath() != null) {
            Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
            FileSystem fs = pendingJobAttemptsPath.getFileSystem(context.getConfiguration());
            // now verify we do not have any files left in the temporary directory structure
            List<Path> fileList = new ArrayList<>();
            boolean containsPendingFiles = containsFiles(fs, pendingJobAttemptsPath, fileList, lenientMode);
            
            if (containsPendingFiles && lenientMode) {
                verifyRemainingTemporaryFilesByName(fs, fileList);
            } else if (containsPendingFiles) {
                throw new FileExistsException("Found files still left in the temporary job attempts path: " + fileList);
            }
        }
        super.cleanupJob(context);
    }
    
    /**
     * Ensure that for each of the non-empty pending files in the provided list there is also a successful file with a matching filename. If not, throw an
     * exception.
     * 
     * @param fs
     *            FileSystem to use
     * @param pendingFileList
     *            List of pending files that need to be verified
     * @throws IOException
     *             for issues with read or write
     * @throws FileExistsException
     *             a pending file exists and there is no successful file with the same name
     */
    private void verifyRemainingTemporaryFilesByName(FileSystem fs, List<Path> pendingFileList) throws IOException {
        List<Path> allFilesInOutputPath = new ArrayList<>();
        
        boolean shouldIgnoreEmptyFiles = true;
        containsFiles(fs, super.getOutputPath(), allFilesInOutputPath, shouldIgnoreEmptyFiles);
        
        LOG.trace("Number of files in output path: {} and in pending: {}", allFilesInOutputPath.size(), pendingFileList.size());
        // Exclude the pending files so that allFilesInOutputPath only contains the successful files
        allFilesInOutputPath.removeAll(pendingFileList);
        LOG.trace("Number of non-pending files: {}", allFilesInOutputPath.size());
        
        // Retrieve just the filenames
        Set<String> successFileNamesOnly = getNames(allFilesInOutputPath);
        Set<String> pendingFileNamesOnly = getNames(pendingFileList);
        
        // Identify which pending filenames do not have a matching successful filename
        pendingFileNamesOnly.removeAll(successFileNamesOnly);
        LOG.trace("successFileNames: {}", successFileNamesOnly);
        
        if (0 < pendingFileNamesOnly.size()) {
            throw new FileExistsException("Found files in temporary job attempts path with no successful counterpart: " + pendingFileNamesOnly);
        }
    }
    
    private Set<String> getNames(List<Path> allFilesInOutputPath) {
        return allFilesInOutputPath.stream().map(Path::getName).collect(Collectors.toSet());
    }
    
    protected boolean containsFiles(final FileSystem fs, final Path path, final List<Path> list) throws FileNotFoundException, IOException {
        return containsFiles(fs, path, list, false);
    }
    
    protected boolean containsFiles(final FileSystem fs, final Path path, final List<Path> list, boolean ignoreEmptyFiles)
                    throws FileNotFoundException, IOException {
        RemoteIterator<Path> listing = listFiles(fs, path, ignoreEmptyFiles);
        while (listing.hasNext()) {
            list.add(listing.next());
        }
        return (!list.isEmpty());
    }
    
    /**
     * I could have used the fs.listFiles(path, true), however that provides the LocatedFileStatus which returns all of the block locations as well as the file
     * status. This is a cheaper iterator which only requests the FileStatus for each file as all we need to know is which paths are files vs directories.
     * 
     * @param fs
     *            the file system
     * @param path
     *            the file path
     * @return A remote iterator of paths for file only
     */
    protected RemoteIterator<Path> listFiles(final FileSystem fs, final Path path) {
        return listFiles(fs, path, false);
    }
    
    /**
     * See SafeFileOutputCommitter.listFiles(FileSystem, Path). When ignoreEmptyFiles is true, listFiles's returned iterator will not return files that are
     * empty.
     * 
     * @param fs
     *            the file system
     * @param path
     *            the file path
     * @param ignoreEmptyFiles
     *            flag to ignore empty files
     * @return A remote iterator of paths
     */
    protected RemoteIterator<Path> listFiles(final FileSystem fs, final Path path, boolean ignoreEmptyFiles) {
        return new RemoteIterator<Path>() {
            private ArrayDeque<FileStatus> files = new ArrayDeque<>();
            private Path curFile = null;
            private boolean initialized = false;
            
            private void initialize() throws IOException {
                if (!initialized) {
                    files.add(fs.getFileStatus(path));
                    initialized = true;
                }
            }
            
            @Override
            public boolean hasNext() throws FileNotFoundException, IOException {
                initialize();
                while (curFile == null && !files.isEmpty()) {
                    FileStatus file = files.removeLast();
                    if (!file.isFile()) {
                        FileStatus[] status = fs.listStatus(file.getPath());
                        Collections.addAll(files, status);
                    } else if (!ignoreEmptyFiles || file.getLen() > 0) {
                        // if ignoreEmptyFiles is true then include all files regardless of size
                        // always include a path if it's a file that's not empty
                        curFile = file.getPath();
                    }
                }
                return curFile != null;
            }
            
            @Override
            public Path next() throws FileNotFoundException, IOException {
                if (hasNext()) {
                    Path result = curFile;
                    curFile = null;
                    return result;
                }
                throw new java.util.NoSuchElementException("No more files under " + path);
            }
        };
    }
    
}
