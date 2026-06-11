package datawave.query.util.sortedmap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.apache.log4j.Logger;

import datawave.query.util.sortedset.FileSortedSet;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * A sorted map that can be persisted into a file and still be read in its persisted state. The map can always be re-loaded and then all operations will work as
 * expected. This class will not support null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 * A RewriteStrategy can be supplied that will determine whether a value gets replaced when putting a key,value pair.
 *
 * @param <K>
 *            key of the map
 * @param <V>
 *            value of the map
 */
public abstract class FileSortedMap<K,V> implements SortedMap<K,V>, Cloneable, RewritableSortedMap<K,V> {
    private static final Logger log = Logger.getLogger(FileSortedMap.class);
    protected boolean persisted;
    protected K[] range;
    protected SortedMap<K,V> map;
    protected RewriteStrategy<K,V> rewriteStrategy = null;

    // The file handler that handles the underlying io
    protected TypedSortedMapFileHandler handler;
    // The sort map factory
    protected FileSortedMapFactory factory;

    /**
     * A class that represents a null object within the map
     */
    public static class NullObject implements Serializable {
        private static final long serialVersionUID = -5528112099317370355L;
    }

    /**
     * Create a file sorted map from another one
     *
     * @param other
     *            the other sorted map
     */
    public FileSortedMap(FileSortedMap<K,V> other) {
        this.handler = other.handler;
        this.factory = other.factory;
        this.map = new TreeMap<>(other.map);
        this.persisted = other.persisted;
        this.range = other.range;
        this.rewriteStrategy = other.rewriteStrategy;
    }

    /**
     * Create a file sorted submap from another one
     *
     * @param other
     *            the other sorted map
     * @param from
     *            the from key
     * @param to
     *            the to key
     */
    public FileSortedMap(FileSortedMap<K,V> other, K from, K to) {
        this(other);
        if (from != null || to != null) {
            if (persisted) {
                this.range = (K[]) new Object[] {getStart(from), getEnd(to)};
            } else if (to == null) {
                this.map = this.map.tailMap(from);
            } else if (from == null) {
                this.map = this.map.headMap(to);
            } else {
                this.map = this.map.subMap(from, to);
            }
        }
    }

    /**
     * Create a persisted sorted map
     *
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     * @param factory
     *            the sorted map factory
     */
    public FileSortedMap(TypedSortedMapFileHandler handler, FileSortedMapFactory factory, boolean persisted) {
        this.handler = handler;
        this.factory = factory;
        this.map = new TreeMap<>();
        this.persisted = persisted;
    }

    /**
     * Create a persisted sorted map
     *
     * @param comparator
     *            the key comparator
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     * @param factory
     *            the sorted map factory
     */
    public FileSortedMap(Comparator<K> comparator, TypedSortedMapFileHandler handler, FileSortedMapFactory factory, boolean persisted) {
        this.handler = handler;
        this.factory = factory;
        this.map = new TreeMap<>(comparator);
        this.persisted = persisted;
    }

    /**
     * Create an unpersisted sorted map (still in memory)
     *
     * @param map
     *            a sorted map
     * @param handler
     *            the sorted map file handler
     * @param factory
     *            the sorted map factory
     */
    public FileSortedMap(SortedMap<K,V> map, TypedSortedMapFileHandler handler, FileSortedMapFactory factory) {
        this.handler = handler;
        this.factory = factory;
        this.map = new TreeMap<>(map);
        this.persisted = false;
        if (map instanceof RewritableSortedMap) {
            setRewriteStrategy(((RewritableSortedMap) map).getRewriteStrategy());
        }
    }

    /**
     * Create a sorted map out of another sorted map. If persist is true, then the map will be directly persisted using the map's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param map
     *            a sorted map
     * @param handler
     *            the sorted map file handler
     * @param factory
     *            the sorted map factory
     * @param persist
     *            the persist boolean flag
     * @throws IOException
     *             for issues with read/write
     */
    public FileSortedMap(SortedMap<K,V> map, TypedSortedMapFileHandler handler, FileSortedMapFactory factory, boolean persist) throws IOException {
        this.handler = handler;
        this.factory = factory;
        if (!persist) {
            this.map = new TreeMap<>(map);
            this.persisted = false;
        } else {
            this.map = new TreeMap<>(map.comparator());
            persist(map, handler);
            persisted = true;
        }
        if (map instanceof RewritableSortedMap) {
            setRewriteStrategy(((RewritableSortedMap) map).getRewriteStrategy());
        }
    }

    @Override
    public RewriteStrategy<K,V> getRewriteStrategy() {
        return rewriteStrategy;
    }

    @Override
    public void setRewriteStrategy(RewriteStrategy<K,V> rewriteStrategy) {
        this.rewriteStrategy = rewriteStrategy;
    }

    /**
     * This will revert this map to whatever contents are in the underlying file, making the map "persisted". This is intended to be used following a load
     * command when no changes were actually made the the map If the persist options included verification, then the files will be verified prior to unloading.
     *
     * @throws IOException
     *             for issues with read/write
     */
    public void unload() throws IOException {
        if (!persisted) {
            verifyPersistance(handler, this.map.size(), Collections.emptyList());
            this.map.clear();
            persisted = true;
        }
    }

    /**
     * This will dump the map to the file, making the map "persisted"
     *
     * @throws IOException
     *             for issues with read/write
     */
    public void persist() throws IOException {
        persist(this.handler);
    }

    /**
     * This will dump the map to the file, making the map "persisted"
     *
     * @param handler
     *            the handler
     * @throws IOException
     *             for issues with read/write
     */
    public void persist(TypedSortedMapFileHandler handler) throws IOException {
        if (!persisted) {
            persist(this.map, handler);
            this.map.clear();
            persisted = true;
        }
    }

    /**
     * This will dump the map to a file, making the map "persisted" The implementation is expected to wrap the handler with a TypedSortedMapFileHandler and the
     * call persist(TypedSortedMapFileHandler handler)
     *
     * @param handler
     *            the sorted map file handler
     * @throws IOException
     *             for issues with read/write
     */
    public abstract void persist(SortedMapFileHandler handler) throws IOException;

    /**
     * Persist the supplied map to a file as defined by this classes sorted map file handler.
     *
     * @param map
     *            the map
     * @param handler
     *            the handler
     * @throws IOException
     *             for issues with read/write
     *
     */
    private void persist(SortedMap<K,V> map, TypedSortedMapFileHandler handler) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Persisting " + handler);
        }

        long start = System.currentTimeMillis();
        try {
            // assign the passed in file handler
            // if we can't persist, we will remap to null
            this.handler = handler;

            int actualSize = 0;
            FileSortedSet.PersistOptions persistOptions = handler.getPersistOptions();
            List<Map.Entry<K,V>> mapToVerify = new ArrayList<>();
            try (SortedMapOutputStream<K,V> stream = handler.getOutputStream()) {
                for (Entry<K,V> t : map.entrySet()) {
                    stream.writeObject(t.getKey(), t.getValue());
                    if (persistOptions.isVerifyElements() && mapToVerify.size() < persistOptions.getNumElementsToVerify()) {
                        mapToVerify.add(t);
                    }
                    actualSize++;
                }
                stream.writeSize(actualSize);
            }
            // now verify the written file
            verifyPersistance(handler, actualSize, mapToVerify);

        } catch (IOException e) {
            handler.deleteFile();
            this.handler = null;
            throw e;
        }

        if (log.isDebugEnabled()) {
            long delta = System.currentTimeMillis() - start;
            log.debug("Persisting " + handler + " took " + delta + "ms");
        }
    }

    private void verifyPersistance(TypedSortedMapFileHandler handler, int size, List<Map.Entry<K,V>> mapToVerify) throws IOException {
        // verify we wrote at least the size....
        if (handler.getSize() == 0) {
            throw new IOException("Failed to verify file existence");
        }
        FileSortedSet.PersistOptions persistOptions = handler.getPersistOptions();
        // now verify the first n objects were written correctly
        if (persistOptions.isVerifyElements() && !mapToVerify.isEmpty()) {
            try (SortedMapInputStream<K,V> inStream = handler.getInputStream()) {
                int count = 0;
                for (Map.Entry<K,V> t : mapToVerify) {
                    count++;
                    Map.Entry<K,V> input = inStream.readObject();
                    if (!equals(t, input)) {
                        throw new IOException("Failed to verify element " + count + " was written");
                    }
                }
            }
        }

        // now verify the size was written at the end
        if (persistOptions.isVerifySize()) {
            if (readSize() != size) {
                throw new IOException("Failed to verify file size was written");
            }
        }
    }

    /**
     * Read the size from the file which is in the last 4 bytes.
     *
     * @return the size (in terms of objects)
     * @throws IOException
     *             for issues with read/write
     */
    private int readSize() throws IOException {
        try (SortedMapInputStream<K,V> inStream = handler.getInputStream()) {
            return inStream.readSize();
        }
    }

    /**
     * This will read the file into an in-memory map, making this file "unpersisted"
     *
     * @throws IOException
     *             for issues with read/write
     * @throws ClassNotFoundException
     *             if the class is not found
     */
    public void load() throws IOException, ClassNotFoundException {
        if (persisted) {
            try (SortedMapInputStream<K,V> stream = getBoundedFileHandler().getInputStream(getStart(), getEnd())) {
                Map.Entry<K,V> obj = stream.readObject();
                while (obj != null) {
                    map.put(obj.getKey(), obj.getValue());
                    obj = stream.readObject();
                }
            }
            persisted = false;
        }
    }

    protected Map.Entry<K,V> readObject(ObjectInputStream stream) {
        try {
            K key = (K) stream.readObject();
            V value = (V) stream.readObject();
            return new UnmodifiableMapEntry(key, value);
        } catch (Exception E) {
            return null;
        }
    }

    protected void writeObject(ObjectOutputStream stream, K key, V value) throws IOException {
        stream.writeObject(key);
        stream.writeObject(value);
    }

    /*
     * Is this map persisted?
     */
    public boolean isPersisted() {
        return persisted;
    }

    /**
     * Get the size of the map. Note if the map has been persisted, then this may be an upper bound on the size.
     *
     * @return the size upper bound
     */
    @Override
    public int size() {
        if (persisted) {
            if (isSubmap()) {
                throw new IllegalStateException("Unable to determine size of a submap of a persisted map.  Please call load() first.");
            }
            try {
                return readSize();
            } catch (Exception e) {
                throw new IllegalStateException("Unable to get size from file", e);
            }
        } else {
            return map.size();
        }
    }

    @Override
    public boolean isEmpty() {
        // must attempt to read the first element to be sure if persisted
        try {
            firstKey();
            return false;
        } catch (NoSuchElementException e) {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object o) {
        if (persisted) {
            K t = (K) o;
            K start = getStart();
            K end = getEnd();
            if ((start != null) && (compare(t, start) < 0)) {
                return false;
            }
            if ((end != null) && (compare(t, end) >= 0)) {
                return false;
            }
            try (SortedMapInputStream<K,V> stream = getBoundedFileHandler().getInputStream(t, end)) {
                Map.Entry<K,V> next = stream.readObject();
                return (next != null && equals(next.getKey(), t));
            } catch (Exception e) {
                return false;
            }
        } else {
            return map.containsKey(o);
        }
    }

    @Override
    public boolean containsValue(Object o) {
        if (persisted) {
            V t = (V) o;
            try (SortedMapInputStream<K,V> stream = getBoundedFileHandler().getInputStream(getStart(), getEnd())) {
                Map.Entry<K,V> next = stream.readObject();
                while (next != null) {
                    if (next.getValue().equals(t)) {
                        return true;
                    }
                    next = stream.readObject();
                }
            } catch (Exception e) {
                return false;
            }
            return false;
        } else {
            return map.containsValue(o);
        }
    }

    @Override
    public V get(Object key) {
        if (persisted) {
            K t = (K) key;
            try (SortedMapInputStream<K,V> stream = getBoundedFileHandler().getInputStream(getStart(), getEnd())) {
                Map.Entry<K,V> next = stream.readObject();
                while (next != null) {
                    if (equals(next.getKey(), t)) {
                        return next.getValue();
                    }
                    next = stream.readObject();
                }
            } catch (Exception e) {
                return null;
            }
            return null;
        } else {
            return map.get(key);
        }
    }

    @Override
    public V put(K key, V value) {
        if (persisted) {
            throw new IllegalStateException("Cannot add an element to a persisted FileSortedMap.  Please call load() first.");
        } else {
            V previous = map.get(key);
            if ((previous == null) || (rewriteStrategy == null) || (rewriteStrategy.rewrite(key, previous, value))) {
                map.put(key, value);
            }
            return previous;
        }
    }

    @Override
    public V remove(Object o) {
        if (persisted) {
            throw new IllegalStateException("Cannot remove an element to a persisted FileSortedMap.  Please call load() first.");
        } else {
            return map.remove(o);
        }
    }

    @Override
    public void putAll(Map<? extends K,? extends V> m) {
        for (Entry<? extends K,? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        if (persisted) {
            handler.deleteFile();
            persisted = false;
        } else {
            map.clear();
        }
    }

    @Override
    public Comparator<? super K> comparator() {
        return map.comparator();
    }

    @Override
    public SortedMap<K,V> subMap(K fromElement, K toElement) {
        return factory.newInstance(this, getStart(fromElement), getEnd(toElement));
    }

    @Override
    public SortedMap<K,V> headMap(K toElement) {
        return factory.newInstance(this, getStart(null), getEnd(toElement));
    }

    @Override
    public SortedMap<K,V> tailMap(K fromElement) {
        return factory.newInstance(this, getStart(fromElement), getEnd(null));
    }

    @Override
    public K firstKey() {
        if (persisted) {
            try (SortedMapInputStream<K,V> stream = getBoundedFileHandler().getInputStream(getStart(), getEnd())) {
                Map.Entry<K,V> first = stream.readObject();
                return first.getKey();
            } catch (Exception e) {
                throw new IllegalStateException(new QueryException(DatawaveErrorCode.FETCH_FIRST_ELEMENT_ERROR, e));
            }
        } else if (!map.isEmpty()) {
            return map.firstKey();
        }
        throw (NoSuchElementException) new NoSuchElementException().initCause(new QueryException(DatawaveErrorCode.FETCH_FIRST_ELEMENT_ERROR));
    }

    @Override
    public K lastKey() {
        if (persisted) {
            try (SortedMapInputStream<K,V> stream = getBoundedFileHandler().getInputStream(getStart(), getEnd())) {
                Map.Entry<K,V> last = stream.readObject();
                Map.Entry<K,V> next = stream.readObject();
                while (next != null) {
                    last = next;
                    next = stream.readObject();
                }
                return last.getKey();
            } catch (Exception e) {
                throw new IllegalStateException(new QueryException(DatawaveErrorCode.FETCH_LAST_ELEMENT_ERROR, e));
            }
        } else if (!map.isEmpty()) {
            return map.lastKey();
        }
        throw (NoSuchElementException) new NoSuchElementException().initCause(new QueryException(DatawaveErrorCode.FETCH_LAST_ELEMENT_ERROR));
    }

    private Iterator<Map.Entry<K,V>> iterator() {
        if (persisted) {
            return new FileIterator();
        } else {
            return map.entrySet().iterator();
        }
    }

    @Override
    public Set<K> keySet() {
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return IteratorUtils.transformedIterator(FileSortedMap.this.iterator(), o -> ((Map.Entry) o).getKey());
            }

            @Override
            public int size() {
                return FileSortedMap.this.size();
            }
        };
    }

    @Override
    public Collection<V> values() {
        return new AbstractCollection<V>() {

            @Override
            public Iterator<V> iterator() {
                return IteratorUtils.transformedIterator(FileSortedMap.this.iterator(), o -> ((Map.Entry) o).getValue());
            }

            @Override
            public int size() {
                return FileSortedMap.this.size();
            }
        };
    }

    @Override
    public Set<Entry<K,V>> entrySet() {
        return new AbstractSet<Entry<K,V>>() {

            @Override
            public Iterator<Entry<K,V>> iterator() {
                return FileSortedMap.this.iterator();
            }

            @Override
            public int size() {
                return FileSortedMap.this.size();
            }
        };
    }

    @Override
    public String toString() {
        return persisted ? handler.toString() : map.toString();
    }

    /**
     * Extending classes must implement cloneable
     *
     * @return A clone
     */
    public FileSortedMap<K,V> clone() {
        return factory.newInstance(this);
    }

    /* Some utilities */
    private boolean equals(Map.Entry<K,V> o1, Map.Entry<K,V> o2) {
        if (o1 == null) {
            return o2 == null;
        } else if (o2 == null) {
            return false;
        } else {
            return equals(o1.getKey(), o2.getKey()) && o1.getValue().equals(o2.getValue());
        }
    }

    private boolean equals(K o1, K o2) {
        if (o1 == null) {
            return o2 == null;
        } else if (o2 == null) {
            return false;
        } else {
            if (map.comparator() == null) {
                return o1.equals(o2);
            } else {
                return map.comparator().compare(o1, o2) == 0;
            }
        }
    }

    private K getStart() {
        return (isSubmap() ? range[0] : null);
    }

    private K getStart(K from) {
        K start = getStart();
        if (start == null) {
            return from;
        } else if (from == null) {
            return start;
        } else if (compare(start, from) > 0) {
            return start;
        } else {
            return from;
        }
    }

    private K getEnd() {
        return (isSubmap() ? range[1] : null);
    }

    private K getEnd(K to) {
        K end = getEnd();
        if (end == null) {
            return to;
        } else if (to == null) {
            return end;
        } else if (compare(end, to) < 0) {
            return end;
        } else {
            return to;
        }
    }

    private boolean isSubmap() {
        return (range != null);
    }

    private int compare(K a, K b) {
        return (this.map.comparator() != null) ? this.map.comparator().compare(a, b) : ((Comparable<K>) a).compareTo(b);
    }

    public BoundedTypedSortedMapFileHandler<K,V> getBoundedFileHandler() {
        return new DefaultBoundedTypedSortedMapFileHandler();
    }

    /**
     * This is the iterator for a persisted FileSortedMap
     */
    protected class FileIterator implements Iterator<Map.Entry<K,V>> {
        private SortedMapInputStream<K,V> stream;
        private Map.Entry<K,V> next;

        public FileIterator() {
            try {
                this.stream = getBoundedFileHandler().getInputStream(getStart(), getEnd());
                next = stream.readObject();
                if (next == null) {
                    cleanup();
                }
            } catch (Exception e) {
                cleanup();
                throw new IllegalStateException("Unable to read file", e);
            }
        }

        public void cleanup() {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception e) {
                    // we tried...
                }
                stream = null;
            }
        }

        @Override
        public boolean hasNext() {
            return (next != null);
        }

        @Override
        public Map.Entry<K,V> next() {
            if (!hasNext()) {
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            try {
                Map.Entry<K,V> rtrn = next;
                next = stream.readObject();
                if (next == null) {
                    cleanup();
                }
                return rtrn;
            } catch (Exception e) {
                cleanup();
                throw new IllegalStateException("Unable to get next element from file", e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Iterator.remove() not supported on a persisted map.");
        }

        @Override
        protected void finalize() throws Throwable {
            cleanup();
            super.finalize();
        }
    }

    /**
     * An interface for a sorted map factory
     *
     * @param <K>
     *            key of the map
     * @param <V>
     *            value of the map
     */
    public interface FileSortedMapFactory<K,V> {
        /**
         * factory method
         *
         * @param other
         *            the other factory
         * @return a new instance
         */
        FileSortedMap<K,V> newInstance(FileSortedMap<K,V> other);

        /**
         * factory method
         *
         * @param other
         *            the other factory
         * @param from
         *            from instance
         * @param to
         *            to instance
         * @return a new instance
         */
        FileSortedMap<K,V> newInstance(FileSortedMap<K,V> other, K from, K to);

        /**
         * Factory method
         *
         * @param comparator
         *            the key comparator
         * @param rewriteStrategy
         *            the rewrite strategy
         * @param handler
         *            the sorted map file handler
         * @param persisted
         *            a persisted boolean flag
         * @return a new instance
         */
        FileSortedMap<K,V> newInstance(Comparator<K> comparator, RewriteStrategy<K,V> rewriteStrategy, SortedMapFileHandler handler, boolean persisted);

        /**
         * Create an unpersisted sorted map (still in memory)
         *
         * @param map
         *            the sorted map
         * @param handler
         *            the sorted map file handler
         * @return a new instance
         */
        FileSortedMap<K,V> newInstance(SortedMap<K,V> map, SortedMapFileHandler handler);

        /**
         * factory method
         *
         * @param map
         *            the sorted map
         * @param handler
         *            the sorted map file handler
         * @param persist
         *            a persisted boolean flag
         * @return a new instance
         * @throws IOException
         *             for problems with read/write
         */
        FileSortedMap<K,V> newInstance(SortedMap<K,V> map, SortedMapFileHandler handler, boolean persist) throws IOException;
    }

    /**
     * A sorted map input stream
     *
     * @param <K>
     *            key of the map
     * @param <V>
     *            value of the map
     */
    public interface SortedMapInputStream<K,V> extends AutoCloseable {
        Map.Entry<K,V> readObject() throws IOException;

        int readSize() throws IOException;

        void close();
    }

    /**
     * A sorted map output stream
     *
     * @param <K>
     *            key of the map
     * @param <V>
     *            value of the map
     */
    public interface SortedMapOutputStream<K,V> extends AutoCloseable {
        void writeObject(K key, V value) throws IOException;

        void writeSize(int size) throws IOException;

        void close() throws IOException;
    }

    /**
     * A factory that will provide the input stream and output stream to the same underlying file.
     *
     */
    public interface SortedMapFileHandler {
        /**
         * Return the input stream
         *
         * @return the input stream
         * @throws IOException
         *             for problems with read/write
         */
        InputStream getInputStream() throws IOException;

        /**
         * Return the output stream
         *
         * @return the sorted map output stream
         * @throws IOException
         *             for problems with read/write
         */
        OutputStream getOutputStream() throws IOException;

        /**
         * Get the persistent verification options
         *
         * @return the persistent verification options
         */
        FileSortedSet.PersistOptions getPersistOptions();

        long getSize();

        void deleteFile();
    }

    /**
     * A factory that will provide the input stream and output stream to the same underlying file.
     *
     */
    public interface TypedSortedMapFileHandler<K,V> {
        /**
         * Return the input stream
         *
         * @return the input stream
         * @throws IOException
         *             for problems with read/write
         */
        SortedMapInputStream<K,V> getInputStream() throws IOException;

        /**
         * Return the output stream
         *
         * @return the sorted map output stream
         * @throws IOException
         *             for problems with read/write
         */
        SortedMapOutputStream<K,V> getOutputStream() throws IOException;

        /**
         * Get the persistent verification options
         *
         * @return persistent verification options
         */
        FileSortedSet.PersistOptions getPersistOptions();

        long getSize();

        void deleteFile();
    }

    /**
     * A factory that will provide the input stream and output stream to the same underlying file. An additional input stream method allows for creating a
     * stream submap.
     *
     */
    public interface BoundedTypedSortedMapFileHandler<K,V> extends TypedSortedMapFileHandler<K,V> {
        /**
         * Return the input stream
         *
         * @return the input stream
         * @param start
         *            start point
         * @param end
         *            end point
         * @throws IOException
         *             for problems with read/write
         */
        SortedMapInputStream<K,V> getInputStream(K start, K end) throws IOException;
    }

    /**
     * A default implementation for a bounded typed sorted map
     */
    public class DefaultBoundedTypedSortedMapFileHandler implements BoundedTypedSortedMapFileHandler<K,V> {
        @Override
        public SortedMapInputStream<K,V> getInputStream(K start, K end) throws IOException {
            if (handler instanceof BoundedTypedSortedMapFileHandler) {
                return ((BoundedTypedSortedMapFileHandler) handler).getInputStream(start, end);
            } else {
                return new BoundedInputStream(handler.getInputStream(), start, end);
            }
        }

        @Override
        public SortedMapInputStream<K,V> getInputStream() throws IOException {
            return handler.getInputStream();
        }

        @Override
        public SortedMapOutputStream<K,V> getOutputStream() throws IOException {
            return handler.getOutputStream();
        }

        @Override
        public FileSortedSet.PersistOptions getPersistOptions() {
            return handler.getPersistOptions();
        }

        @Override
        public long getSize() {
            return handler.getSize();
        }

        @Override
        public void deleteFile() {
            handler.deleteFile();
        }
    }

    /**
     * An input stream that supports bounding the objects. Used when the underlying stream does not already support bounding.
     */
    public class BoundedInputStream implements SortedMapInputStream<K,V> {
        private final SortedMapInputStream<K,V> delegate;
        private final K from;
        private final K to;

        public BoundedInputStream(SortedMapInputStream<K,V> stream, K from, K to) {
            this.delegate = stream;
            this.from = from;
            this.to = to;
        }

        @Override
        public Map.Entry<K,V> readObject() throws IOException {
            Map.Entry<K,V> o = delegate.readObject();
            while ((o != null) && (from != null) && (compare(o.getKey(), from) < 0)) {
                o = delegate.readObject();
            }
            if (o == null || (to != null && compare(o.getKey(), to) >= 0)) {
                return null;
            } else {
                return o;
            }
        }

        @Override
        public int readSize() throws IOException {
            return delegate.readSize();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    public interface RewriteStrategy<K,V> {
        /**
         * Determine if the object should be rewritten
         *
         * @param key
         *            The key
         * @param original
         *            The original value
         * @param update
         *            The updated value
         * @return true of the original should be replaced with the update
         */
        boolean rewrite(K key, V original, V update);
    }

}
