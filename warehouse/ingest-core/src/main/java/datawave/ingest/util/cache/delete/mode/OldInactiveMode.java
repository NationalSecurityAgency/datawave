package datawave.ingest.util.cache.delete.mode;

import com.google.common.base.Predicates;
import datawave.ingest.util.cache.JobCacheFactory;
import datawave.ingest.util.cache.delete.DeleteJobCacheLauncher;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Delete job cache mode that will attempt to delete caches that are old and inactive */
public class OldInactiveMode implements DeleteJobCacheMode {
    private static final Logger LOGGER = LoggerFactory.getLogger(OldInactiveMode.class);
    
    @Override
    public Collection<FileSystemPath> getDeletionCandidates(Collection<Configuration> hadoopConfs, DeleteModeOptions options) {
        Collection<FileSystemPath> jobCachePaths = FileSystemPath.getFileSystemPaths(options.getJobCachePaths(), hadoopConfs);
        Collection<Path> keepPaths = options.getKeepPaths();
        Pattern timestampPattern = options.getTimestampPattern();
        int keepNumVersions = options.getKeepNumVersions();
        
        LOGGER.debug("{} mode. Job base path: {} , Keep paths: {} , pattern: {} and num versions {}", Mode.OLD_INACTIVE, jobCachePaths, keepPaths,
                        timestampPattern, keepNumVersions);
        
        // @formatter:off
        return jobCachePaths
                .stream()
                .map(jobCachePath -> JobCacheFactory.getCacheCandidates(jobCachePath, timestampPattern, keepNumVersions))
                .flatMap(Collection::stream)
                .filter(Predicates.not(fsPath -> keepPaths.contains(fsPath.getOutputPath())))
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    @Override
    public DeleteJobCacheMode.Mode getMode() {
        return Mode.OLD_INACTIVE;
    }
}
