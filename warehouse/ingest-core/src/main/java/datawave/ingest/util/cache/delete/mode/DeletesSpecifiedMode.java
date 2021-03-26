package datawave.ingest.util.cache.delete.mode;

import datawave.ingest.util.cache.delete.DeleteJobCache;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Collection;

/** Delete job cache mode that will attempt to delete caches specified by the user */
public class DeletesSpecifiedMode implements DeleteJobCacheMode {
    
    @Override
    public Collection<FileSystemPath> getDeletionCandidates(Collection<Configuration> hadoopConfs, DeleteModeOptions options) {
        Collection<Path> deletePaths = options.getDeletePaths();
        Collection<Path> keepPaths = options.getKeepPaths();
        
        Collection<Path> deleteCandidates = DeleteJobCache.getDeletionCandidates(deletePaths, keepPaths);
        return FileSystemPath.getFileSystemPaths(deleteCandidates, hadoopConfs);
    }
    
    @Override
    public Mode getMode() {
        return Mode.DELETES_SPECIFIED;
    }
}
