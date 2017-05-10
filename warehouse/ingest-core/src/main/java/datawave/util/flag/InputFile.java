package datawave.util.flag;

import java.util.Comparator;

import org.apache.hadoop.fs.Path;

/**
 * Wrapper for input file meta data
 */
public class InputFile implements Comparable<InputFile> {
    
    long blocksize;
    long filesize;
    private Path path;
    private long timestamp;
    private String folder;
    
    public InputFile() {}
    
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
     */
    public InputFile(String folder, Path path, long blocksize, long filesize, long timestamp) {
        this.folder = folder;
        this.path = path;
        this.blocksize = blocksize;
        this.filesize = filesize;
        this.timestamp = timestamp;
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
    
    public void setFilesize(int filesize) {
        this.filesize = filesize;
    }
    
    public String getDirectory() {
        return path.getParent().toString();
    }
    
    public Path getPath() {
        return path;
    }
    
    public String getFolder() {
        return folder;
    }
    
    public int getMaps() {
        double maps = (blocksize == 0 ? 1 : (double) filesize / blocksize);
        return (int) Math.ceil(maps);
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
        if (this.path != other.path && (this.path == null || !this.path.equals(other.path))) {
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
        return "InputFile{" + "blocksize=" + blocksize + ", filesize=" + filesize + ", timestamp=" + timestamp + ", path=" + path + '}';
    }
    
    /**
     * A FIFO comparator
     */
    public static final Comparator<InputFile> FIFO = new Comparator<InputFile>() {
        
        @Override
        public int compare(InputFile o1, InputFile o2) {
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
        }
        
    };
    
    /**
     * A LIFO comparator
     */
    public static final Comparator<InputFile> LIFO = new Comparator<InputFile>() {
        
        @Override
        public int compare(InputFile o1, InputFile o2) {
            // simply a reverse of the FIFO comparison
            return FIFO.compare(o2, o1);
        }
        
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
    
}
