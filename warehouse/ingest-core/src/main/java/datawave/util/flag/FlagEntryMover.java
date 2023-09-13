package datawave.util.flag;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

import datawave.util.flag.InputFile.TrackedDir;

/**
 * Handles stage 1 of the job creation flow. Checks for destination directories, creates as needed, and returns information on whether it was successful.
 */
public class FlagEntryMover extends SimpleMover {

    private static final Logger log = LoggerFactory.getLogger(FlagEntryMover.class);
    private static final int CHECKSUM_MAX = 10 * 1024 * 1000; // 10M

    public FlagEntryMover(Cache<Path,Path> directoryCache, FileSystem fs, InputFile entry) {
        super(directoryCache, entry, TrackedDir.FLAGGING_DIR, fs);
    }

    @Override
    public InputFile call() throws IOException {

        // Create the flagging, flagged and loaded directory if they do not exist
        Path dstFlagging = checkParent(entry.getFlagging());
        Path dstFlagged = checkParent(entry.getFlagged());
        Path dstLoaded = checkParent(entry.getLoaded());

        // Check for existence of the file already in the flagging, flagged, or loaded directories
        Path src = entry.getPath();
        boolean doMove = true;
        if (fs.exists(dstFlagging)) {
            doMove = resolveConflict(src, dstFlagging);
        } else if (fs.exists(dstFlagged)) {
            doMove = resolveConflict(src, dstFlagged);
        } else if (fs.exists(dstLoaded)) {
            doMove = resolveConflict(src, dstLoaded);
        }

        // do the move
        if (doMove) {
            super.call();
        }

        return entry;
    }

    /**
     * Resolves the ingest file name to a unique file name for ingestion.
     *
     * @param src
     *            source file for ingestion
     * @param dest
     *            conflict file
     * @return true/false if the checksum matches
     * @throws IOException
     *             for issues with read/write
     */
    private boolean resolveConflict(final Path src, final Path dest) throws IOException {
        // check to see if checksum matches
        boolean resolved = false;
        long srcLen = this.fs.getFileStatus(src).getLen();
        long destLen = this.fs.getFileStatus(dest).getLen();
        if (srcLen == destLen) {
            String sumSrc = calculateChecksum(src);
            String sumDest = calculateChecksum(dest);
            if (!sumSrc.equals(sumDest)) {
                resolved = true;
            }
        } else {
            resolved = true;
        }

        if (resolved) {
            // rename tracked locations
            log.warn("duplicate ingest file name with different payload({}) - appending timestamp to destination file name", src.toUri());
            this.entry.renameTrackedLocations();
        } else {
            log.warn("discarding duplicate ingest file ({}) duplicate ({})", src.toUri(), dest.toUri());
            if (!fs.delete(src, false)) {
                log.error("unable to delete duplicate ingest file ({})", src.toUri());
            }
        }

        return resolved;
    }

    /**
     * Calculates a checksum for a file.
     *
     * @param file
     *            checksum file
     * @return hex string representation of checksum
     * @throws IOException
     *             for issues with read/write
     */
    private String calculateChecksum(final Path file) throws IOException {
        try (final InputStream is = this.fs.open(file)) {
            byte[] buf = new byte[8096];
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
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
