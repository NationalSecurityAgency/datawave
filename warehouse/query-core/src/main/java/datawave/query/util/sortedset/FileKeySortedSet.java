package datawave.query.util.sortedset;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.SortedSet;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeySortedSet extends FileSortedSet<Key> {
    private static Logger log = Logger.getLogger(FileKeySortedSet.class);
    
    /**
     * Create a file sorted set from another one
     * 
     * @param other
     */
    public FileKeySortedSet(FileKeySortedSet other) {
        super(other);
    }
    
    /**
     * Create a persisted sorted set
     * 
     * @param handler
     * @param persisted
     */
    public FileKeySortedSet(SortedSetFileHandler handler, boolean persisted) {
        super(handler, persisted);
    }
    
    /**
     * Create a persistede sorted set
     * 
     * @param comparator
     * @param handler
     * @param persisted
     */
    public FileKeySortedSet(Comparator<? super Key> comparator, SortedSetFileHandler handler, boolean persisted) {
        super(comparator, handler, persisted);
    }
    
    /**
     * Create an unpersisted sorted set (still in memory)
     * 
     * @param set
     * @param handler
     */
    public FileKeySortedSet(SortedSet<Key> set, SortedSetFileHandler handler) {
        super(set, handler);
    }
    
    /**
     * Create an sorted set out of another sorted set. If persist is true, then the set will be directly persisted using the set's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param set
     * @param handler
     */
    public FileKeySortedSet(SortedSet<Key> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, handler, persist);
    }
    
    /**
     * Get an input stream
     * 
     * @return the input stream
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Override
    protected DataInputStream getInputStream() throws IOException {
        return new DataInputStream(new BufferedInputStream(handler.getInputStream()));
    }
    
    /**
     * Get an output stream
     * 
     * @return the output stream
     * @throws IOException
     */
    @Override
    protected DataOutputStream getOutputStream() throws IOException {
        return new DataOutputStream(new BufferedOutputStream(handler.getOutputStream()));
    }
    
    /**
     * Write T to an object output stream
     * 
     * @param stream
     * @param t
     * @throws IOException
     */
    @Override
    protected void writeObject(OutputStream stream, Key t) throws IOException {
        t.write((DataOutputStream) stream);
    }
    
    /**
     * Read T from an object input stream
     *
     * @param stream
     * @return a key
     * @throws IOException
     */
    @Override
    protected Key readObject(InputStream stream) throws IOException {
        Key o = new Key();
        o.readFields((DataInputStream) stream);
        return o;
    }
    
    /**
     * Clone this set
     */
    @Override
    public FileKeySortedSet clone() {
        return new FileKeySortedSet(this);
    }
    
    /**
     * A factory for these file sorted sets
     */
    public static class Factory implements FileSortedSetFactory<Key> {
        
        @Override
        public FileKeySortedSet newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedSet(handler, persisted);
        }
        
        @Override
        public FileKeySortedSet newInstance(Comparator<? super Key> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedSet(comparator, handler, persisted);
        }
        
        @Override
        public FileKeySortedSet newInstance(SortedSet<Key> set, SortedSetFileHandler handler) {
            return new FileKeySortedSet(set, handler);
        }
        
        @Override
        public FileKeySortedSet newInstance(SortedSet<Key> set, SortedSetFileHandler handler, boolean persist) throws IOException {
            return new FileKeySortedSet(set, handler, persist);
        }
    }
}
