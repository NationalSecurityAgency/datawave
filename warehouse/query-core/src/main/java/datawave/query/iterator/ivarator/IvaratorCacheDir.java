package datawave.query.iterator.ivarator;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IvaratorCacheDir {

    private static final Logger log = LoggerFactory.getLogger(IvaratorCacheDir.class);

    final protected IvaratorCacheDirConfig config;

    // the filesystem to use for this ivarator cache dir
    final protected FileSystem fs;

    // the path for caching ivarator output for this query
    final protected String pathURI;

    public IvaratorCacheDir(IvaratorCacheDirConfig config, FileSystem fs, String pathURI) {
        this.config = config;
        this.fs = fs;
        this.pathURI = pathURI;
    }

    public IvaratorCacheDirConfig getConfig() {
        return config;
    }

    public FileSystem getFs() {
        return fs;
    }

    public boolean validateFS() {
        FsStatus fsStatus = null;
        try {
            fsStatus = getFs().getStatus();
        } catch (IOException e) {
            log.warn("Unable to determine status of the filesystem: {}", getFs());
        }

        // determine whether this fs is a good candidate
        if (fsStatus != null) {
            long availableStorageMiB = fsStatus.getRemaining() / 0x100000L;
            double availableStoragePercent = (double) fsStatus.getRemaining() / fsStatus.getCapacity();

            // if we are using less than our storage limit, the cache dir is valid
            return availableStorageMiB >= config.getMinAvailableStorageMiB() && availableStoragePercent >= config.getMinAvailableStoragePercent();
        }

        return false;
    }

    public String getPathURI() {
        return pathURI;
    }

    @Override
    public String toString() {
        return pathURI;
    }
}
