package datawave.query.util.sortedset;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.SortedSet;
import org.apache.log4j.Logger;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileSerializableSortedSet<E extends Serializable> extends FileSortedSet<E> {
    private static Logger log = Logger.getLogger(FileSerializableSortedSet.class);
    
    /**
     * Create a file sorted set from another one
     *
     * @param other
     */
    public FileSerializableSortedSet(FileSerializableSortedSet other) {
        super(other);
    }
    
    /**
     * Create a persisted sorted set
     *
     * @param handler
     * @param persisted
     */
    public FileSerializableSortedSet(SortedSetFileHandler handler, boolean persisted) {
        super(handler, persisted);
    }
    
    /**
     * Create a persistede sorted set
     *
     * @param comparator
     * @param handler
     * @param persisted
     */
    public FileSerializableSortedSet(Comparator<? super E> comparator, SortedSetFileHandler handler, boolean persisted) {
        super(comparator, handler, persisted);
    }
    
    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     * @param handler
     */
    public FileSerializableSortedSet(SortedSet<E> set, SortedSetFileHandler handler) {
        super(set, handler);
    }
    
    /**
     * Create an sorted set out of another sorted set. If persist is true, then the set will be directly persisted using the set's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param set
     * @param handler
     */
    public FileSerializableSortedSet(SortedSet<E> set, SortedSetFileHandler handler, boolean persist) throws IOException {
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
    protected ObjectInputStream getInputStream() throws IOException {
        return new ObjectInputStream(new BufferedInputStream(handler.getInputStream()));
    }
    
    /**
     * Get an output stream
     * 
     * @return the output stream
     * @throws IOException
     */
    @Override
    protected ObjectOutputStream getOutputStream() throws IOException {
        return new ObjectOutputStream(new BufferedOutputStream(handler.getOutputStream()));
    }
    
    /**
     * Write KeyValueSerializable to an object output stream
     * 
     * @param stream
     * @param t
     * @throws IOException
     */
    @Override
    protected void writeObject(OutputStream stream, E t) throws IOException {
        ((ObjectOutputStream) stream).writeObject(t);
    }
    
    /**
     * Read KeyValueSerializable from an object input stream
     *
     * @param stream
     * @return a key
     * @throws IOException
     */
    @Override
    protected E readObject(InputStream stream) throws IOException {
        try {
            Object o = ((ObjectInputStream) stream).readObject();
            return (E) o;
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not deserialize object", e);
        }
    }
    
    @Override
    public FileSortedSet<E> clone() {
        return new FileSerializableSortedSet(this);
    }
    
    /**
     * A factory for this set
     */
    public static class Factory<E extends Serializable> implements FileSortedSetFactory<E> {
        
        @Override
        public FileSerializableSortedSet<E> newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileSerializableSortedSet(handler, persisted);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(Comparator<? super E> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileSerializableSortedSet(comparator, handler, persisted);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(SortedSet<E> set, SortedSetFileHandler handler) {
            return new FileSerializableSortedSet(set, handler);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(SortedSet<E> set, SortedSetFileHandler handler, boolean persist) throws IOException {
            return new FileSerializableSortedSet(set, handler, persist);
        }
    }
    
}
