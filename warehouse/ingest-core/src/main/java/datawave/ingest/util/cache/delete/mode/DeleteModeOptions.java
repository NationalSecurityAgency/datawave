package datawave.ingest.util.cache.delete.mode;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import datawave.ingest.util.cache.converter.HadoopPathConverter;
import datawave.ingest.util.cache.converter.RegexPatternConverter;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/** JCommander options class that will evaluate command line delete mode options. */
public class DeleteModeOptions {
    @Parameter(names = {"--delete-paths"}, description = "A list of timestamp job cache paths to delete.", converter = HadoopPathConverter.class)
    List<Path> deletePaths = Lists.newArrayList();
    
    @Parameter(names = {"--job-cache-paths"}, description = "The parent job cache directories to evaluate.", converter = HadoopPathConverter.class)
    List<Path> jobCachePaths;
    
    @Parameter(names = {"--keep-num-versions"}, description = "The number of old cache directories to keep.")
    int keepNumVersions = 1;
    
    @Parameter(names = {"--keep-paths"}, description = "A list of timestamp directories to keep.", converter = HadoopPathConverter.class)
    List<Path> keepPaths = Lists.newArrayList();
    
    @Parameter(names = {"--timestamp-pattern"}, description = "The timestamp pattern.", converter = RegexPatternConverter.class)
    Pattern timestampPattern = Pattern.compile("jobCache_\\d{14}");
    
    public Collection<Path> getDeletePaths() {
        return deletePaths;
    }
    
    public Collection<Path> getJobCachePaths() {
        return jobCachePaths;
    }
    
    public int getKeepNumVersions() {
        return keepNumVersions;
    }
    
    public Collection<Path> getKeepPaths() {
        return keepPaths;
    }
    
    public Pattern getTimestampPattern() {
        return timestampPattern;
    }
    
}
