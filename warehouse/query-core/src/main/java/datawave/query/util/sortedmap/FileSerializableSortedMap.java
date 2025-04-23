package datawave.query.util.sortedmap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;
import org.apache.log4j.Logger;

import datawave.query.util.sortedset.FileSortedSet;

/**
 * A sorted map that can be persisted into a file and still be read in its persisted state. The map can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying maps iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 * @param <K>
 *            key of the map
 * @param <V>
 *            value of the map
 */
public class FileSerializableSortedMap<K extends Serializable,V extends Serializable> extends FileSortedMap<K,V> {
    private static Logger log = Logger.getLogger(FileSerializableSortedMap.class);

    /**
     * Create a file sorted map from another one
     *
     * @param other
     *            the other sorted map
     */
    public FileSerializableSortedMap(FileSerializableSortedMap other) {
        super(other);
    }

    /**
     * Create a file sorted submap from another one
     *
     * @param other
     *            the other sorted map
     * @param from
     *            the from file
     * @param to
     *            the to file
     */
    public FileSerializableSortedMap(FileSerializableSortedMap other, K from, K to) {
        super(other, from, to);
    }

    /**
     * Create a persisted sorted map
     *
     * @param handler
     *            a file handler
     * @param persisted
     *            persisted boolean flag
     */
    public FileSerializableSortedMap(TypedSortedMapFileHandler handler, boolean persisted) {
        super(handler, new Factory(), persisted);
    }

    /**
     * Create a persistede sorted map
     *
     * @param comparator
     *            a comparator
     * @param handler
     *            a file handler
     * @param persisted
     *            persisted boolean flag
     */
    public FileSerializableSortedMap(Comparator<K> comparator, TypedSortedMapFileHandler handler, boolean persisted) {
        super(comparator, handler, new Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted map (still in memory)
     *
     * @param map
     *            a sorted map
     * @param handler
     *            a file handler
     */
    public FileSerializableSortedMap(SortedMap<K,V> map, TypedSortedMapFileHandler handler) {
        super(map, handler, new Factory());
    }

    /**
     * Create an sorted map out of another sorted map. If persist is true, then the map will be directly persisted using the map's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param map
     *            a sorted map
     * @param handler
     *            a file handler
     * @param persist
     *            a persist flag
     * @throws IOException
     *             for issues with read/write
     */
    public FileSerializableSortedMap(SortedMap<K,V> map, TypedSortedMapFileHandler handler, boolean persist) throws IOException {
        super(map, handler, new Factory(), persist);
    }

    /**
     * Persist a map using the specified handler
     *
     * @param handler
     *            a file handler
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    public void persist(SortedMapFileHandler handler) throws IOException {
        super.persist(new SerializableFileHandler(handler));
    }

    @Override
    public FileSerializableSortedMap<K,V> clone() {
        return (FileSerializableSortedMap) super.clone();
    }

    /**
     * A SortedMapfilehandler that can handler serializable objects
     */
    public static class SerializableFileHandler<K extends Serializable,V extends Serializable> implements TypedSortedMapFileHandler<K,V> {
        SortedMapFileHandler delegate;

        public SerializableFileHandler(SortedMapFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedMapInputStream<K,V> getInputStream() throws IOException {
            return new SerializableInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedMapOutputStream getOutputStream() throws IOException {
            return new SerializableOutputStream(delegate.getOutputStream());
        }

        @Override
        public FileSortedSet.PersistOptions getPersistOptions() {
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

    public static class SerializableInputStream<K extends Serializable,V extends Serializable> implements SortedMapInputStream<K,V> {
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
        public Map.Entry<K,V> readObject() throws IOException {
            try {
                K key = (K) getDelegate().readObject();
                V value = (V) getDelegate().readObject();
                return new UnmodifiableMapEntry<>(key, value);
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

            // read the 4 bytes of an integer in a deterministic order0
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

    public static class SerializableOutputStream<K extends Serializable,V extends Serializable> implements FileSortedMap.SortedMapOutputStream<K,V> {
        private ObjectOutputStream delegate;

        public SerializableOutputStream(OutputStream stream) throws IOException {
            delegate = new ObjectOutputStream(stream);
        }

        @Override
        public void writeObject(K key, V value) throws IOException {
            delegate.writeObject(key);
            delegate.writeObject(value);
        }

        @Override
        public void writeSize(int size) throws IOException {
            // write the 4 bytes of a integer in a deterministic order0
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
     * A factory for this map
     */
    public static class Factory<K extends Serializable,V extends Serializable> implements FileSortedMapFactory<K,V> {

        @Override
        public FileSerializableSortedMap<K,V> newInstance(FileSortedMap<K,V> other) {
            return new FileSerializableSortedMap((FileSerializableSortedMap) other);
        }

        @Override
        public FileSerializableSortedMap<K,V> newInstance(FileSortedMap<K,V> other, K from, K to) {
            return new FileSerializableSortedMap((FileSerializableSortedMap) other, from, to);
        }

        @Override
        public FileSerializableSortedMap<K,V> newInstance(Comparator<K> comparator, RewriteStrategy<K,V> rewriteStrategy, SortedMapFileHandler handler,
                        boolean persisted) {
            FileSerializableSortedMap map = new FileSerializableSortedMap(comparator, new SerializableFileHandler(handler), persisted);
            map.setRewriteStrategy(rewriteStrategy);
            return map;
        }

        @Override
        public FileSortedMap<K,V> newInstance(SortedMap<K,V> map, SortedMapFileHandler handler) {
            return new FileSerializableSortedMap(map, new SerializableFileHandler(handler));
        }

        @Override
        public FileSortedMap<K,V> newInstance(SortedMap<K,V> map, SortedMapFileHandler handler, boolean persist) throws IOException {
            return new FileSerializableSortedMap(map, new SerializableFileHandler(handler), persist);
        }
    }

}
