package datawave.ingest.util.cache.mode;

import com.beust.jcommander.Parameter;

/** JCommander options class that will define the mode options for LoadJobCacheLauncher */
public class ModeOptions {
    
    private static final String DATAWAVE_HOME = System.getenv().getOrDefault("DATAWAVE_HOME", "/opt/datawave-ingest/current/");
    
    @Parameter(names = {"--classpath-base-dir"}, description = "Base directory for relative classpath entries.")
    String classpathBaseDir = DATAWAVE_HOME + "bin/ingest";
    
    @Parameter(names = {"--file-pattern"}, description = "In file pattern mode, the pattern of files to upload.")
    String filePattern = "**";
    
    @Parameter(names = {"--input-path"}, description = "In file pattern mode, the directory to search for files.")
    String inputPath = DATAWAVE_HOME + "lib/";
    
    @Parameter(names = {"--max-depth"}, description = "In file pattern mode, the maximum depth for a directory search.")
    int maxDepth = Integer.MAX_VALUE;
    
    public String getClasspathBaseDir() {
        return classpathBaseDir;
    }
    
    public String getFilePattern() {
        return filePattern;
    }
    
    public int getMaxDepth() {
        return maxDepth;
    }
    
    public String getInputPath() {
        return inputPath;
    }
}
