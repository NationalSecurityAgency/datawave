package nsa.datawave.poller.filter;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;

/**
 * A FilenameFilter implementation to ignore any extraneous files that shouldn't be auto-added into HDFS from the poller output completed directory. Currently
 * uses a blacklist of filenames to determine whether a file should be accepted.
 * 
 */
public class CompletedFileFilter implements FilenameFilter {
    private HashSet<String> ignoredFilenames;
    
    public CompletedFileFilter() {
        ignoredFilenames = new HashSet<>();
        
        init();
    }
    
    /**
     * Build blacklist of file names
     */
    public void init() {
        ignoredFilenames.add(".DS_Store");
    }
    
    @Override
    public boolean accept(File dir, String name) {
        // Make sure the filename isn't in the blacklist
        return !ignoredFilenames.contains(name);
    }
    
}
