package datawave.util.flag;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

import datawave.util.flag.InputFile.TrackedDir;

/**
 * Handles stage 1 of the job creation flow.
 * Ensures destination directories exist.
 * Detects duplicate files in destination directories, deletes duplicates, renames non-duplicate files with the same name.
 */
public class FlagEntryMover extends SimpleMover {

    private static final Logger LOG = LoggerFactory.getLogger(FlagEntryMover.class);
    private static final int CHECKSUM_MAX = 10 * 1024 * 1000; // 10M
    private static final String CHECKSUM_ALGORITHM = "SHA-256";

    public FlagEntryMover(Cache<Path,Path> directoryCache, FileSystem fs, InputFile entry) {
        super(directoryCache, entry, TrackedDir.FLAGGING_DIR, fs);
    }

    @Override
    public InputFile call() throws IOException {
        Path src = entry.getPath();
        Path[] destinations = {entry.getFlagging(), entry.getFlagged(), entry.getLoaded()};

        createAndCache(destinations);
        if (noDuplicatesExist(src, destinations)) {
            super.call();
        }

        return entry;
    }

    private boolean noDuplicatesExist(Path src, Path[] destinations) throws IOException {
        // Check if a file with the same filename also exists in the flagging, flagged, or loaded directories
        for (Path destination : destinations) {
            if (fs.exists(destination)) {
                // todo - look for other conflicts?
                return resolvedConflict(src, destination);
            }
        }
        return true;
    }

    private void createAndCache(Path[] destinations) throws IOException {
        for (Path destination : destinations) {
            super.checkParent(destination);
        }
    }

    /**
     * Called only if the src and dest filenames match.
     * Resolves the ingest file name to a unique file name for ingestion.
     *
     * @param src source file for ingestion
     * @param dest file with the same name as src in a different directory
     * @return true/false if the checksums match
     * @throws IOException for issues with read/write
     */
    private boolean resolvedConflict(final Path src, final Path dest) throws IOException {
        if (fileContentsMatch(src, dest)) {
            LOG.warn("Discarding duplicate ingest file ({}) duplicate ({})", src.toUri(), dest.toUri());
            if (!fs.delete(src, false)) {
                LOG.error("Unable to delete duplicate ingest file ({})", src.toUri());
            }
            return false;
        }

        LOG.warn("Duplicate ingest file name with different payload({}) - appending timestamp to destination file names", src.toUri());
        this.entry.renameTrackedLocations();
        return true;
    }

    private boolean fileContentsMatch(Path src, Path dest) throws IOException {
        return haveEqualByteLengths(src, dest) && haveMatchingChecksums(src, dest);
    }

    private boolean haveEqualByteLengths(Path src, Path dest) throws IOException {
        long srcNumBytes = this.fs.getFileStatus(src).getLen();
        long destNumBytes = this.fs.getFileStatus(dest).getLen();
        return srcNumBytes == destNumBytes;
    }

    private boolean haveMatchingChecksums(Path src, Path dest) throws IOException {
        String sumSrc = getChecksum(src);
        String sumDest = getChecksum(dest);
        return sumSrc.equals(sumDest);
    }

    /**
     * Calculates a checksum for a file only if the filesystem checksum isn't implemented.
     *
     * @param file
     *            checksum file
     * @return hex string representation of checksum
     * @throws IOException
     *             for issues with read/write
     */
    private String getChecksum(final Path file) throws IOException {
        FileChecksum result = null;
        try {
            result = fs.getFileChecksum(file);
        } catch (IOException e) {
            LOG.warn("Failed to get checksum on {}", file);
            LOG.warn("getFileChecksum failure", e);
        }

        if (result != null) {
            return result.toString();
        }

        LOG.trace("Checksum unavailable for filesystem, calculating manually");
        return calculateChecksumManually(file);
    }

    private String calculateChecksumManually(Path file) throws IOException {
        try (final InputStream is = this.fs.open(file)) {
            byte[] buf = new byte[8096];
            MessageDigest digest = MessageDigest.getInstance(CHECKSUM_ALGORITHM);
            int len;
            long totalLen = 0;
            // use CHECKSUM_MAX as limit for checksum
            while (-1 != (len = is.read(buf)) && CHECKSUM_MAX > totalLen) {
                digest.update(buf, 0, len);
                totalLen += len;
            }

            byte[] mdBytes = digest.digest();
            final StringBuilder sb = new StringBuilder();
            for (byte mdByte : mdBytes) {
                sb.append(Integer.toString((mdByte & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();
        } catch (NoSuchAlgorithmException ae) {
            throw new IOException(ae);
        }
    }
}
