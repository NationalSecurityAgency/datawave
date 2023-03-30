package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.SortedSet;
import org.apache.accumulo.core.data.Key;
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
     *            the other sorted set
     */
    public FileSerializableSortedSet(FileSerializableSortedSet other) {
        super(other);
    }
    
    /**
     * Create a file sorted subset from another one
     *
     * @param other
     *            the other sorted set
     * @param from
     *            the from file
     * @param to
     *            the to file
     */
    public FileSerializableSortedSet(FileSerializableSortedSet other, E from, E to) {
        super(other, from, to);
    }
    
    /**
     * Create a persisted sorted set
     *
     * @param handler
     *            a file handler
     * @param persisted
     *            persisted boolean flag
     */
    public FileSerializableSortedSet(TypedSortedSetFileHandler handler, boolean persisted) {
        super(handler, new Factory(), persisted);
    }
    
    /**
     * Create a persistede sorted set
     *
     * @param comparator
     *            a comparator
     * @param handler
     *            a file handler
     * @param persisted
     *            persisted boolean flag
     */
    public FileSerializableSortedSet(Comparator<? super E> comparator, TypedSortedSetFileHandler handler, boolean persisted) {
        super(comparator, handler, new Factory(), persisted);
    }
    
    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     *            a sorted set
     * @param handler
     *            a file handler
     */
    public FileSerializableSortedSet(SortedSet<E> set, TypedSortedSetFileHandler handler) {
        super(set, handler, new Factory());
    }
    
    /**
     * Create an sorted set out of another sorted set. If persist is true, then the set will be directly persisted using the set's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param set
     *            a sorted set
     * @param handler
     *            a file handler
     * @param persist
     *            a persist flag
     * @throws IOException
     *             for issues with read/write
     */
    public FileSerializableSortedSet(SortedSet<E> set, TypedSortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, handler, new Factory(), persist);
    }
    
    /**
     * Persist a set using the specified handler
     *
     * @param handler
     *            a file handler
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    public void persist(SortedSetFileHandler handler) throws IOException {
        super.persist(new SerializableFileHandler(handler));
    }
    
    @Override
    public FileSerializableSortedSet<E> clone() {
        return (FileSerializableSortedSet) super.clone();
    }
    
    /**
     * A sortedsetfilehandler that can handler serializable objects
     */
    public static class SerializableFileHandler<E> implements TypedSortedSetFileHandler<Key> {
        SortedSetFileHandler delegate;
        
        public SerializableFileHandler(SortedSetFileHandler handler) {
            this.delegate = handler;
        }
        
        @Override
        public SortedSetInputStream<Key> getInputStream() throws IOException {
            return new SerializableInputStream(delegate.getInputStream(), delegate.getSize());
        }
        
        @Override
        public SortedSetOutputStream getOutputStream() throws IOException {
            return new SerializableOutputStream(delegate.getOutputStream());
        }
        
        @Override
        public PersistOptions getPersistOptions() {
            return delegate.getPersistOptions();
        }
        
        @Override
        public long getSize() {
            return delegate.getSize();
        }
        
        @Override
        public void deleteFile() {
            delegate.deleteFile();
        }
    }
    
    public static class SerializableInputStream<E> implements SortedSetInputStream<E> {
        private final InputStream stream;
        private ObjectInputStream delegate;
        private final long length;
        
        public SerializableInputStream(InputStream stream, long length) throws IOException {
            this.stream = stream;
            this.length = length;
        }
        
        private ObjectInputStream getDelegate() throws IOException {
            if (delegate == null) {
                this.delegate = new ObjectInputStream(stream);
            }
            return delegate;
        }
        
        @Override
        public E readObject() throws IOException {
            try {
                return (E) getDelegate().readObject();
            } catch (IOException ioe) {
                return null;
            } catch (ClassNotFoundException nnfe) {
                return null;
            }
        }
        
        @Override
        public int readSize() throws IOException {
            long bytesToSkip = length - 4;
            long total = 0;
            long cur = 0;
            
            while ((total < bytesToSkip) && ((cur = stream.skip(bytesToSkip - total)) > 0)) {
                total += cur;
            }
            
            byte[] buffer = new byte[4];
            stream.read(buffer);
            
            return ((buffer[3] & 0xFF)) + ((buffer[2] & 0xFF) << 8) + ((buffer[1] & 0xFF) << 16) + ((buffer[0]) << 24);
        }
        
        @Override
        public void close() {
            try {
                if (delegate != null) {
                    delegate.close();
                } else {
                    stream.close();
                }
            } catch (Exception e) {
                log.error("Failed to close input stream", e);
            }
        }
    }
    
    public static class SerializableOutputStream<E> implements SortedSetOutputStream<E> {
        private ObjectOutputStream delegate;
        
        public SerializableOutputStream(OutputStream stream) throws IOException {
            delegate = new ObjectOutputStream(stream);
        }
        
        @Override
        public void writeObject(E obj) throws IOException {
            delegate.writeObject(obj);
        }
        
        @Override
        public void writeSize(int size) throws IOException {
            delegate.write((size >>> 24) & 0xFF);
            delegate.write((size >>> 16) & 0xFF);
            delegate.write((size >>> 8) & 0xFF);
            delegate.write((size >>> 0) & 0xFF);
        }
        
        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
    
    /**
     * A factory for this set
     */
    public static class Factory<E extends Serializable> implements FileSortedSetFactory<E> {
        
        @Override
        public FileSerializableSortedSet<E> newInstance(FileSortedSet<E> other) {
            return new FileSerializableSortedSet((FileSerializableSortedSet) other);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(FileSortedSet<E> other, E from, E to) {
            return new FileSerializableSortedSet((FileSerializableSortedSet) other, from, to);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileSerializableSortedSet(new SerializableFileHandler(handler), persisted);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(Comparator<? super E> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileSerializableSortedSet(comparator, new SerializableFileHandler(handler), persisted);
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(SortedSet<E> set, SortedSetFileHandler handler) {
            return new FileSerializableSortedSet(set, new SerializableFileHandler(handler));
        }
        
        @Override
        public FileSerializableSortedSet<E> newInstance(SortedSet<E> set, SortedSetFileHandler handler, boolean persist) throws IOException {
            return new FileSerializableSortedSet(set, new SerializableFileHandler(handler), persist);
        }
    }
    
}
