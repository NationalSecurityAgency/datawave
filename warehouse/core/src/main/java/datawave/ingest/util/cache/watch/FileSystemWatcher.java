package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 *
 */
public abstract class FileSystemWatcher<V> extends Reloadable<V> {

    protected long lastChange = -1;

    protected Path filePath;

    protected long configuredDiff;

    protected FileSystem fs;

    private static final Logger log = Logger.getLogger(FileSystemWatcher.class);

    public FileSystemWatcher(FileSystem fs, Path filePath, long configuredDiff) throws IOException {
        this.fs = fs;
        this.filePath = filePath;
        this.configuredDiff = configuredDiff;

    }

    /**
     * @param filePath
     * @return
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    private byte[] checksumFile(Path filePath) throws NoSuchAlgorithmException, IOException {

        FileChecksum checksum = fs.getFileChecksum(filePath);

        if (null == checksum) {
            return "".getBytes();
        } else
            return checksum.getBytes();
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.util.cache.watcher.Watcher#hasChanged()
     */
    @Override
    public boolean hasChanged() {

        boolean reload = false;

        long currentModTime;
        try {
            currentModTime = fs.getFileStatus(filePath).getModificationTime();
            // |= not necessary

            reload = (currentModTime - lastChange) > configuredDiff;
            if (log.isDebugEnabled())
                log.debug(currentModTime + " " + lastChange + " " + configuredDiff + " reload triggered?" + reload);
        } catch (IOException e) {
            reload = true;
        }

        return reload;
    }

    protected abstract V loadContents(InputStream stream) throws IOException;

    public boolean equals(Object obj) {
        if (obj instanceof FileSystemWatcher) {
            FileSystemWatcher<V> otherWatcher = ((FileSystemWatcher<V>) obj);
            return filePath.equals(otherWatcher.filePath) && configuredDiff == otherWatcher.configuredDiff;
        }
        return false;
    }

    public int hashCode() {
        return 31 + filePath.hashCode() + (int) configuredDiff;
    }

    public long getLastChange() {
        return lastChange;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.util.cache.watch.Reloadable#reload()
     */
    @Override
    public V reload() {

        try {

            lastChange = fs.getFileStatus(filePath).getModificationTime();
            if (log.isDebugEnabled())
                log.debug("Reload called " + lastChange);

            FSDataInputStream in = fs.open(filePath);
            V result = loadContents(in);
            in.close();
            return result;

        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

}
