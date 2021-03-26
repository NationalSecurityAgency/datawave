package datawave.ingest.util.cache.delete.mode;

import com.google.common.base.Predicates;
import datawave.ingest.util.cache.delete.DeleteJobCache;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Delete job cache mode that will attempt to delete caches that are old and inactive */
public class OldInactiveMode implements DeleteJobCacheMode {
    
    @Override
    public Collection<FileSystemPath> getDeletionCandidates(Collection<Configuration> hadoopConfs, DeleteModeOptions options) {
        Collection<FileSystemPath> jobCachePaths = FileSystemPath.getFileSystemPaths(options.getJobCachePaths(), hadoopConfs);
        Collection<Path> keepPaths = options.getKeepPaths();
        Pattern timestampPattern = options.getTimestampPattern();
        int keepNumVersions = options.getKeepNumVersions();
        
        // @formatter:off
        return jobCachePaths
                .stream()
                .map(jobCachePath -> DeleteJobCache.getDeletionCandidates(jobCachePath, timestampPattern, keepNumVersions))
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
