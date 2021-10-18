package datawave.ingest.mapreduce.job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
    
    public static final String ALLOW_EQUIVALENT_FILENAME = "mapreduce.safefileoutputcommitter.allow.equivalent.filename";
    
    private static final boolean DEFAULT_ALLOW_EQUIVALENT_FILENAME = false;
    private final boolean shouldFindEquivalentFilename;
    
    // a boolean denoting whether we should check for an empty directory before cleaning it up
    private volatile boolean checkForEmptyDir = true;
    
    public SafeFileOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
        this.shouldFindEquivalentFilename = context.getConfiguration().getBoolean(ALLOW_EQUIVALENT_FILENAME, DEFAULT_ALLOW_EQUIVALENT_FILENAME);
    }
    
    public SafeFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
        this.shouldFindEquivalentFilename = context.getConfiguration().getBoolean(ALLOW_EQUIVALENT_FILENAME, DEFAULT_ALLOW_EQUIVALENT_FILENAME);
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
     */
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        if (checkForEmptyDir && super.getOutputPath() != null) {
            Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
            FileSystem fs = pendingJobAttemptsPath.getFileSystem(context.getConfiguration());
            // now verify we do not have any files left in the temporary directory structure
            List<Path> fileList = new ArrayList<>();
            boolean containsPendingFiles = containsFiles(fs, pendingJobAttemptsPath, fileList);
            
            if (containsPendingFiles && !shouldFindEquivalentFilename) {
                throw new FileExistsException("Found files still left in the temporary job attempts path: " + fileList);
            }
            if (containsPendingFiles) {
                verifyRemainingTemporaryFiles(fs, fileList);
            }
        }
        super.cleanupJob(context);
    }

    /**
     * Ensure that for each of the pending files in the provided list there is also a successful file with a matching filename.  If not, throw an exception.
     * @param fs FileSystem to use
     * @param pendingFileList List of pending files that need to be verified
     * @throws IOException
     * @throws FileExistsException a pending file exists and there is no successful file with the same name
     */
    private void verifyRemainingTemporaryFiles(FileSystem fs, List<Path> pendingFileList) throws IOException {
        List<Path> allFilesInOutputPath = new ArrayList<>();
        containsFiles(fs, super.getOutputPath(), allFilesInOutputPath);
        
        LOG.trace("Number of files in output path: {} and in pending: {}", allFilesInOutputPath.size(), pendingFileList.size());
        // Exclude the pending files so that allFilesInOutputPath only contains the successful files
        allFilesInOutputPath.removeAll(pendingFileList);
        LOG.trace("Number of non-pending files: {}", allFilesInOutputPath.size());

        // Retrieve just the filenames
        Collection<String> successFileNamesOnly = allFilesInOutputPath.stream().map(Path::getName).collect(Collectors.toList());
        Collection<String> pendingFileNamesOnly = pendingFileList.stream().map(Path::getName).collect(Collectors.toList());
        // Identify which pending filenames do not have a matching successful filename
        pendingFileNamesOnly.removeAll(successFileNamesOnly);
        if (0 < pendingFileNamesOnly.size()) {
            throw new FileExistsException("Found files in temporary job attempts path with no successful counterpart: " + pendingFileNamesOnly);
        }
    }
    
    protected boolean containsFiles(final FileSystem fs, final Path path, final List<Path> list) throws FileNotFoundException, IOException {
        RemoteIterator<Path> listing = listFiles(fs, path);
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
     * @param path
     * @return A remote iterator of paths for file only
     */
    protected RemoteIterator<Path> listFiles(final FileSystem fs, final Path path) {
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
                    if (file.isFile()) {
                        curFile = file.getPath();
                    } else {
                        FileStatus[] status = fs.listStatus(file.getPath());
                        Collections.addAll(files, status);
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
