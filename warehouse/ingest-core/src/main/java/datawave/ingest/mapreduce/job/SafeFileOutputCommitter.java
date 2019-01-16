package datawave.ingest.mapreduce.job;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 * This extension of the FileOutputCommitter does a safety check to ensure no files were left around in the temporary directories before terminating. If there
 * were, then this will fail the job.
 */
public class SafeFileOutputCommitter extends FileOutputCommitter {
    
    // a copy of the super classes output path since they keep it private
    private Path outputPath = null;
    // a boolean denoting whether we should check for an empty directory before cleaning it up
    private volatile boolean checkForEmptyDir = true;
    
    public SafeFileOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
        
        // Since the parent class hides its "outputPath", we need to recreate it here
        if (outputPath != null) {
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
            this.outputPath = fs.makeQualified(outputPath);
        }
    }
    
    public SafeFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
        
        // Since the parent class hides its "outputPath", we need to recreate it here
        if (outputPath != null) {
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
            this.outputPath = fs.makeQualified(outputPath);
        }
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
        return new Path(this.outputPath, PENDING_DIR_NAME);
    }
    
    /**
     * Cleanup the job. Note that this is deprecated in the super class but is still being used for this work. When the method has been removed from the super
     * class then this class will need to be reworked.
     */
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        if (checkForEmptyDir) {
            if (this.outputPath != null) {
                Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
                FileSystem fs = pendingJobAttemptsPath.getFileSystem(context.getConfiguration());
                // now verify we do not have any files left in the temporary directory structure
                List<Path> fileList = new ArrayList<>();
                if (containsFiles(fs, pendingJobAttemptsPath, fileList)) {
                    throw new FileExistsException("Found files still left in the temporary job attempts path: " + fileList);
                }
            }
        }
        super.cleanupJob(context);
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
