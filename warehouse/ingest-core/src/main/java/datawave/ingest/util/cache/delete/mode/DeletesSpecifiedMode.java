package datawave.ingest.util.cache.delete.mode;

import datawave.ingest.util.cache.delete.DeleteJobCache;
import datawave.ingest.util.cache.delete.DeleteJobCacheLauncher;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/** Delete job cache mode that will attempt to delete caches specified by the user */
public class DeletesSpecifiedMode implements DeleteJobCacheMode {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeletesSpecifiedMode.class);
    
    @Override
    public Collection<FileSystemPath> getDeletionCandidates(Collection<Configuration> hadoopConfs, DeleteModeOptions options) {
        Collection<Path> deletePaths = options.getDeletePaths();
        Collection<Path> keepPaths = options.getKeepPaths();
        
        LOGGER.debug("{} mode. Delete paths: {} , Keep paths: {}", Mode.DELETES_SPECIFIED, deletePaths, keepPaths);
        Collection<Path> deleteCandidates = DeleteJobCache.getDeletionCandidates(deletePaths, keepPaths);
        return FileSystemPath.getFileSystemPaths(deleteCandidates, hadoopConfs);
    }
    
    @Override
    public Mode getMode() {
        return Mode.DELETES_SPECIFIED;
    }
}
