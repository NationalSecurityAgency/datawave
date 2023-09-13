package datawave.util.flag;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datawave.util.flag.processor.DateUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for input file meta data
 */
public class InputFile implements Comparable<InputFile> {

    private static final Logger log = LoggerFactory.getLogger(InputFile.class);
    static final String DATE_FORMAT_STRING = "yyyy" + File.separator + "MM" + File.separator + "dd" + File.separator + "HH";
    // our yyyy/MM/dd{/HH{/mm{/ss}}} pattern for most things.
    public static final Pattern PATTERN = Pattern.compile(".*/([0-9]{4}(/[0-9]{2}){2,5})(?:/.*|$)");

    /**
     * Defines the tracked directory locations within the flag hdfs.
     */
    enum TrackedDir {
        // @formatter:off
        PATH_DIR("path"),
        FLAGGING_DIR("flagging"),
        FLAGGED_DIR("flagged"),
        LOADED_DIR("loaded");
        // @formatter:on

        private final String path;

        TrackedDir(String path) {
            this.path = path;
        }
    }

    private long blocksize;
    private long filesize;
    private Path path;
    private long timestamp;
    private String folder;

    // flag file paths
    private Path flagging;
    private Path flagged;
    private Path loaded;

    // state variables
    private TrackedDir currentDir;
    private boolean moved;

    // public InputFile() {}

    /**
     * Create an InputFile
     *
     * @param folder
     *            The folder that was originally searched in
     * @param path
     *            The actual file full path
     * @param blocksize
     *            The blocksize
     * @param filesize
     *            the filesize
     * @param timestamp
     *            the last modified timestamp
     * @param baseDir
     *            base directory
     */
    InputFile(String folder, Path path, long blocksize, long filesize, long timestamp, String baseDir) {
        this.folder = folder;
        this.path = path;
        this.blocksize = blocksize;
        this.filesize = filesize;
        this.timestamp = timestamp;

        this.flagging = getDestPath(path, TrackedDir.FLAGGING_DIR.path, baseDir, folder);
        this.flagged = getDestPath(path, TrackedDir.FLAGGED_DIR.path, baseDir, folder);
        this.loaded = getDestPath(path, TrackedDir.LOADED_DIR.path, baseDir, folder);
        this.currentDir = TrackedDir.PATH_DIR;
    }

    InputFile(String folder, FileStatus status, String baseDir, boolean useFolderTimestamp) {
        this(folder, status.getPath(), status.getBlockSize(), status.getLen(), createTimestamp(status.getPath(), status.getModificationTime(),
                        useFolderTimestamp), baseDir);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getBlocksize() {
        return blocksize;
    }

    public String getFileName() {
        return path.getName();
    }

    public long getFilesize() {
        return filesize;
    }

    public void setFilesize(long filesize) {
        this.filesize = filesize;
    }

    public String getDirectory() {
        return path.getParent().toString();
    }

    Path getFlagging() {
        return flagging;
    }

    public Path getFlagged() {
        return flagged;
    }

    public Path getLoaded() {
        return loaded;
    }

    public Path getPath() {
        return path;
    }

    public String getFolder() {
        return folder;
    }

    /**
     * @return the number of blocks the filesize is expected to use
     */
    public int getMaps() {
        double maps = (blocksize == 0 ? 1 : (double) filesize / blocksize);
        return (int) Math.ceil(maps);
    }

    public Path getCurrentDir() {
        return getTrackedDir(this.currentDir);
    }

    public boolean isMoved() {
        return moved;
    }

    void setMoved(boolean moved) {
        this.moved = moved;
    }

    // ===============================================
    // utility methods
    /**
     * Indicates the file has been moved to one of the tracked locations.
     *
     * @param update
     *            updated location for file
     */
    void updateCurrentDir(TrackedDir update) {
        this.currentDir = update;
        this.moved = true;
    }

    /**
     * Returns the {@link Path} for the file in the specific tracked directory.
     *
     * @param loc
     *            tracked directory
     * @return path for the file
     */
    Path getTrackedDir(TrackedDir loc) {
        Path file = null;
        switch (loc) {
            case PATH_DIR:
                file = this.path;
                break;
            case FLAGGING_DIR:
                file = this.flagging;
                break;
            case FLAGGED_DIR:
                file = this.flagged;
                break;
            case LOADED_DIR:
                file = this.loaded;
        }

        return file;
    }

    /**
     * Returns the {@link Path} for the file in the specific tracked directory.
     *
     * @param loc
     *            tracked directory
     * @return path for the file in the specific tracked directory.
     */
    int getTrackedDirLength(TrackedDir loc) {
        Path file = getTrackedDir(loc);
        return file.toUri().toString().length();
    }

    /**
     * Updates the tracked locations to use the current timestamp to resolve naming conflicts.
     */
    void renameTrackedLocations() {
        final String now = "-" + System.currentTimeMillis();
        this.flagged = this.flagged.suffix(now);
        this.flagging = this.flagging.suffix(now);
        this.loaded = this.loaded.suffix(now);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final InputFile other = (InputFile) obj;
        if (this.timestamp != other.timestamp) {
            return false;
        }
        if (this.blocksize != other.blocksize) {
            return false;
        }
        if (this.filesize != other.filesize) {
            return false;
        }
        if (!Objects.equals(this.path, other.path)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + (int) (this.blocksize ^ (this.blocksize >>> 32));
        hash = 53 * hash + (int) (this.filesize ^ (this.filesize >>> 32));
        hash = 53 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
        hash = 53 * hash + (this.path != null ? this.path.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        // @formatter:off
        return "InputFile{" +
                "blocksize=" + blocksize +
                ", filesize=" + filesize +
                ", path=" + path +
                ", timestamp=" + timestamp +
                ", folder='" + folder + '\'' +
                ", flagging=" + flagging +
                ", flagged=" + flagged +
                ", loaded=" + loaded +
                ", currentDir=" + currentDir +
                ", moved=" + moved +
                '}';
        // @formatter:on
    }

    /**
     * A FIFO comparator
     */
    public static final Comparator<InputFile> FIFO = (o1, o2) -> {
        int comparison = 0;
        if (o1.timestamp < o2.timestamp) {
            comparison = -1;
        } else if (o1.timestamp > o2.timestamp) {
            comparison = 1;
        }
        if (comparison == 0) {
            if (o1.filesize < o2.filesize) {
                comparison = -1;
            } else if (o1.filesize > o2.filesize) {
                comparison = 1;
            }
        }
        if (comparison == 0) {
            if (o1.blocksize < o2.blocksize) {
                comparison = -1;
            } else if (o1.blocksize > o2.blocksize) {
                comparison = 1;
            }
        }
        if (comparison == 0) {
            comparison = o1.path.compareTo(o2.path);
        }
        return comparison;
    };

    /**
     * A LIFO comparator
     */
    public static final Comparator<InputFile> LIFO = (o1, o2) -> {
        // simply a reverse of the FIFO comparison
        return FIFO.compare(o2, o1);
    };

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(InputFile o) {
        return FIFO.compare(this, o);
    }

    /**
     * if it matches the date pattern, use that, otherwise use a pattern of now
     *
     * @param inFile
     *            the input file
     * @param baseDir
     *            base directory
     * @param folder
     *            a folder
     * @param subdir
     *            the subdirectory
     * @return destination file
     */
    static Path getDestPath(Path inFile, String subdir, String baseDir, String folder) {
        Matcher m = PATTERN.matcher(inFile.getParent().toString());
        StringBuilder dstPath = new StringBuilder(baseDir);
        appendWithSep(dstPath, subdir);
        appendWithSep(dstPath, folder);
        if (m.find()) {
            appendWithSep(dstPath, m.group(1));
        } else {
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT_STRING);
            appendWithSep(dstPath, format.format(new Date()));
        }
        return new Path(appendWithSep(dstPath, inFile.getName()).toString());
    }

    private static StringBuilder appendWithSep(StringBuilder b, String next) {
        if (b.charAt(b.length() - 1) != File.separatorChar) {
            b.append(File.separatorChar);
        }
        b.append(next);
        return b;
    }

    /**
     * Returns a timestamp to use for the specified {@link Path} entry.
     *
     * @param path
     *            entry for determination of timestamp
     * @param fileTimestamp
     *            default timestamp for file
     * @param useFolderTimestamp
     *            if true then use the path of the file to determine the timestamp
     * @return timestamp value
     */
    private static long createTimestamp(Path path, long fileTimestamp, boolean useFolderTimestamp) {
        // if using the folder timestamp, then pull the day out of the folder timestamp
        try {
            return useFolderTimestamp ? DateUtils.getFolderTimestamp(path.toString()) : fileTimestamp;
        } catch (Exception e) {
            log.warn("Path does not contain yyyy/mm/dd...using file timestamp for {}", path);
            return fileTimestamp;
        }
    }
}
