package datawave.query.util.sortedmap;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import datawave.query.util.sortedmap.rfile.RFileByteIntegerInputStream;
import datawave.query.util.sortedmap.rfile.RFileByteIntegerOutputStream;
import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * A sorted map that can be persisted into a file and still be read in its persisted state. The map can always be reloaded and then all operations will work as
 * expected. This will support null contained in the underlying maps iff a comparator is supplied that can handle null values.
 */
public class FileByteIntegerSortedMap extends FileSortedMap<byte[],Integer> {

    private static final Logger log = Logger.getLogger(FileByteIntegerSortedMap.class);

    public final static class DefaultByteComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] o1, byte[] o2) {
            return new ByteArrayComparator().compare(o1, o2);
        }
    }

    /**
     * Create a file sorted map from another map.
     *
     * @param other
     *            the other map
     */
    public FileByteIntegerSortedMap(FileByteIntegerSortedMap other) {
        super(other);
    }

    /**
     * Create a file sorted sub-map from another map.
     *
     * @param other
     *            the other map
     * @param from
     *            the from key
     * @param to
     *            the to key
     */
    public FileByteIntegerSortedMap(FileByteIntegerSortedMap other, byte[] from, byte[] to) {
        super(other, from, to);
    }

    /**
     * Create a persisted sorted map.
     *
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileByteIntegerSortedMap(SortedMapFileHandler handler, boolean persisted) {
        this(new DefaultByteComparator(), handler, persisted);
    }

    /**
     * Create a persisted sorted map.
     *
     * @param comparator
     *            the key comparator
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileByteIntegerSortedMap(Comparator<byte[]> comparator, SortedMapFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultByteComparator() : comparator), new ByteIntegerFileHandler(handler), new Factory(), persisted);
    }

    /**
     * Create a non-persisted sorted map (still in memory).
     *
     * @param map
     *            the sorted map
     * @param handler
     *            the sorted map file handler
     */
    public FileByteIntegerSortedMap(SortedMap<byte[],Integer> map, SortedMapFileHandler handler) {
        super(map, new ByteIntegerFileHandler(handler), new Factory());
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
     */
    public FileByteIntegerSortedMap(SortedMap<byte[],Integer> map, SortedMapFileHandler handler, boolean persist) throws IOException {
        super(map, new ByteIntegerFileHandler(handler), new Factory(), persist);
    }

    /**
     * Dumps the map to the file, making the map "persisted".
     *
     * @param handler
     *            the sorted map file handler
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    public void persist(SortedMapFileHandler handler) throws IOException {
        // Ensure this handler is wrapped with the ByteInteger file handler.
        super.persist(new ByteIntegerFileHandler(handler));
    }

    /**
     * Return a clone of this map.
     *
     * @return the clone
     */
    public FileByteIntegerSortedMap clone() {
        return (FileByteIntegerSortedMap) super.clone();
    }

    /**
     * A {@link datawave.query.util.sortedmap.FileSortedMap.BoundedTypedSortedMapFileHandler} that can bound the input stream.
     */
    public static class ByteIntegerFileHandler implements BoundedTypedSortedMapFileHandler<byte[],Integer> {

        SortedMapFileHandler delegate;

        public ByteIntegerFileHandler(SortedMapFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedMapInputStream<byte[],Integer> getInputStream() throws IOException {
            return new RFileByteIntegerInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedMapInputStream<byte[],Integer> getInputStream(byte[] start, byte[] end) throws IOException {
            return new RFileByteIntegerInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedMapOutputStream<byte[],Integer> getOutputStream() throws IOException {
            return new RFileByteIntegerOutputStream(delegate.getOutputStream());
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
     * A factory for these file sorted maps.
     */
    public static class Factory implements FileSortedMapFactory<byte[],Integer> {

        @Override
        public FileSortedMap<byte[],Integer> newInstance(FileSortedMap<byte[],Integer> other) {
            return new FileByteIntegerSortedMap((FileByteIntegerSortedMap) other);
        }

        @Override
        public FileSortedMap<byte[],Integer> newInstance(FileSortedMap<byte[],Integer> other, byte[] from, byte[] to) {
            return new FileByteIntegerSortedMap((FileByteIntegerSortedMap) other, from, to);
        }

        @Override
        public FileSortedMap<byte[],Integer> newInstance(Comparator<byte[]> comparator, RewriteStrategy<byte[],Integer> rewriteStrategy,
                        SortedMapFileHandler handler, boolean persisted) {
            FileByteIntegerSortedMap map = new FileByteIntegerSortedMap(comparator, handler, persisted);
            map.setRewriteStrategy(rewriteStrategy);
            return map;
        }

        @Override
        public FileSortedMap<byte[],Integer> newInstance(SortedMap<byte[],Integer> map, SortedMapFileHandler handler) {
            return new FileByteIntegerSortedMap(map, handler);
        }

        @Override
        public FileSortedMap<byte[],Integer> newInstance(SortedMap<byte[],Integer> map, SortedMapFileHandler handler, boolean persist) throws IOException {
            return new FileByteIntegerSortedMap(map, handler, persist);
        }
    }
}
