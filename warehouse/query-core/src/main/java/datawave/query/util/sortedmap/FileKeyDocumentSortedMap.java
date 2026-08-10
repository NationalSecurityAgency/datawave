package datawave.query.util.sortedmap;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.rfile.RFileKeyDocumentInputStream;
import datawave.query.util.sortedmap.rfile.RFileKeyDocumentOutputStream;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * A sorted map that can be persisted into a file and still be read in its persisted state. The map can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying maps iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeyDocumentSortedMap extends FileSortedMap<Key,Document> {
    private static Logger log = Logger.getLogger(FileKeyDocumentSortedMap.class);

    public static class DefaultKeyComparator implements Comparator<Key> {
        @Override
        public int compare(Key o1, Key o2) {
            return o1.compareTo(o2);
        }
    }

    /**
     * Create a file sorted map from another one
     *
     * @param other
     *            the other sorted map
     */
    public FileKeyDocumentSortedMap(FileKeyDocumentSortedMap other) {
        super(other);
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
    public FileKeyDocumentSortedMap(FileKeyDocumentSortedMap other, Key from, Key to) {
        super(other, from, to);
    }

    /**
     * Create a persisted sorted map
     *
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileKeyDocumentSortedMap(SortedMapFileHandler handler, boolean persisted) {
        this(new DefaultKeyComparator(), handler, persisted);
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
     */
    public FileKeyDocumentSortedMap(Comparator<Key> comparator, SortedMapFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultKeyComparator() : comparator), new KeyDocumentFileHandler(handler), new Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted map (still in memory)
     *
     * @param map
     *            the sorted map
     * @param handler
     *            the sorted map file handler
     */
    public FileKeyDocumentSortedMap(SortedMap<Key,Document> map, SortedMapFileHandler handler) {
        super(map, new KeyDocumentFileHandler(handler), new Factory());
    }

    /**
     * Create a sorted map out of another sorted map. If persist is true, then the map will be directly persisted using the map's iterator which avoids pulling
     * all of its entries into memory at once.
     *
     * @param map
     *            the sorted map
     * @param handler
     *            the sorted map file handler
     * @param persist
     *            boolean flag for persist
     * @throws IOException
     *             for issues with read/write
     */
    public FileKeyDocumentSortedMap(SortedMap<Key,Document> map, SortedMapFileHandler handler, boolean persist) throws IOException {
        super(map, new KeyDocumentFileHandler(handler), new Factory(), persist);
    }

    /**
     * This will dump the map to the file, making the map "persisted"
     *
     * @param handler
     *            the sorted map file handler
     * @throws IOException
     *             for issues with read/write
     */
    public void persist(SortedMapFileHandler handler) throws IOException {
        // ensure this handler is wrapped with our handler
        super.persist(new KeyDocumentFileHandler(handler));
    }

    /**
     * Clone this map
     */
    @Override
    public FileKeyDocumentSortedMap clone() {
        return (FileKeyDocumentSortedMap) super.clone();
    }

    /**
     * A SortedMapfilehandler that can bound the input stream
     */
    public static class KeyDocumentFileHandler implements BoundedTypedSortedMapFileHandler<Key,Document> {
        SortedMapFileHandler delegate;

        public KeyDocumentFileHandler(SortedMapFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedMapInputStream<Key,Document> getInputStream() throws IOException {
            return new RFileKeyDocumentInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedMapInputStream<Key,Document> getInputStream(Key start, Key end) throws IOException {
            return new RFileKeyDocumentInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedMapOutputStream getOutputStream() throws IOException {
            return new RFileKeyDocumentOutputStream(delegate.getOutputStream());
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

    /**
     * A factory for these file sorted maps
     */
    public static class Factory implements FileSortedMapFactory<Key,Document> {

        @Override
        public FileKeyDocumentSortedMap newInstance(FileSortedMap<Key,Document> other) {
            return new FileKeyDocumentSortedMap((FileKeyDocumentSortedMap) other);
        }

        @Override
        public FileKeyDocumentSortedMap newInstance(FileSortedMap<Key,Document> other, Key from, Key to) {
            return new FileKeyDocumentSortedMap((FileKeyDocumentSortedMap) other, from, to);
        }

        @Override
        public FileSortedMap<Key,Document> newInstance(Comparator<Key> comparator, RewriteStrategy<Key,Document> rewriteStrategy, SortedMapFileHandler handler,
                        boolean persisted) {
            FileKeyDocumentSortedMap map = new FileKeyDocumentSortedMap(comparator, handler, persisted);
            map.setRewriteStrategy(rewriteStrategy);
            return map;
        }

        @Override
        public FileKeyDocumentSortedMap newInstance(SortedMap<Key,Document> map, SortedMapFileHandler handler) {
            return new FileKeyDocumentSortedMap(map, handler);
        }

        @Override
        public FileKeyDocumentSortedMap newInstance(SortedMap<Key,Document> map, SortedMapFileHandler handler, boolean persist) throws IOException {
            return new FileKeyDocumentSortedMap(map, handler, persist);
        }
    }
}
