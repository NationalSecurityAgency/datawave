package datawave.util.flag;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

import datawave.util.flag.InputFile.TrackedDir;

/**
 * Moves an entry to it's next targeted location. Updates current location and move status upon completion.
 */
public class SimpleMover implements Callable<InputFile> {

    private static final Logger log = LoggerFactory.getLogger(SimpleMover.class);

    final InputFile entry;
    final TrackedDir target;
    final FileSystem fs;
    final Cache<Path,Path> directoryCache;

    public SimpleMover(Cache<Path,Path> directoryCache, InputFile entry, TrackedDir destination, FileSystem fs) {
        this.directoryCache = directoryCache;
        this.entry = entry;
        this.target = destination;
        this.fs = fs;
        this.entry.setMoved(false);
    }

    @Override
    public InputFile call() throws IOException {
        final Path dst = this.entry.getTrackedDir(this.target);
        checkParent(dst);
        if (entry.getCurrentDir() == dst || (!fs.exists(dst) && fs.rename(entry.getCurrentDir(), dst))) {
            log.trace("Moved from {} to {}", entry.getPath(), dst);
            entry.updateCurrentDir(this.target);
        } else {
            log.error("Unable to move file {} to {}, skipping", entry.getCurrentDir().toUri(), dst.toUri());
        }

        return entry;
    }

    Path checkParent(Path path) throws IOException {
        Path parent = path.getParent();
        if (directoryCache.getIfPresent(parent) == null && !fs.exists(parent)) {
            if (fs.mkdirs(parent)) {
                directoryCache.put(parent, parent);
            } else {
                log.warn("unable to create directory ({})", parent);
            }
        }
        return path;
    }
}
