package datawave.ingest.util.cache.load.mode;

import com.beust.jcommander.Parameter;

/** JCommander options class that will define the mode options for LoadJobCacheLauncher */
public class LoadModeOptions {
    
    private static final String DATAWAVE_HOME = System.getenv().getOrDefault("DATAWAVE_HOME", "/opt/datawave-ingest/current/");
    
    @Parameter(names = {"--classpath"}, description = "Classpath value.")
    String classpath;
    
    @Parameter(names = {"--file-pattern"}, description = "In file pattern mode, the pattern of files to upload.")
    String filePattern = "**";
    
    @Parameter(names = {"--input-path"}, description = "In file pattern mode, the directory to search for files.")
    String inputPath = DATAWAVE_HOME + "lib/";
    
    @Parameter(names = {"--max-depth"}, description = "In file pattern mode, the maximum depth for a directory search.")
    int maxDepth = Integer.MAX_VALUE;
    
    public String getClasspath() {
        return classpath;
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
