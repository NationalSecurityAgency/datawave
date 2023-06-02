package datawave.query.util.sortedset;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.util.sortedset.rfile.RFileByteDocumentInputStream;
import datawave.query.util.sortedset.rfile.RFileByteDocumentOutputStream;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileByteDocumentSortedSet extends FileSortedSet<Map.Entry<byte[],Document>> {
    private static Logger log = Logger.getLogger(FileByteDocumentSortedSet.class);

    public final static class DefaultByteDocumentComparator implements Comparator<Map.Entry<byte[],Document>> {

        @Override
        public int compare(Map.Entry<byte[],Document> o1, Map.Entry<byte[],Document> o2) {
            return new ByteArrayComparator().compare(o1.getKey(), o2.getKey());
        }
    }

    /**
     * Create a file sorted set from another one
     *
     * @param other
     *            the other sorted set
     */
    public FileByteDocumentSortedSet(FileByteDocumentSortedSet other) {
        super(other);
    }

    /**
     * Create a file sorted subset from another one
     *
     * @param other
     *            the other sorted set
     * @param from
     *            the from Document
     * @param to
     *            the to Document
     */
    public FileByteDocumentSortedSet(FileByteDocumentSortedSet other, Map.Entry<byte[],Document> from, Map.Entry<byte[],Document> to) {
        super(other, from, to);
    }

    /**
     * Create a persisted sorted set
     *
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileByteDocumentSortedSet(SortedSetFileHandler handler, boolean persisted) {
        this(new DefaultByteDocumentComparator(), handler, persisted);
    }

    /**
     * Create a persisted sorted set
     *
     * @param comparator
     *            the key comparator
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileByteDocumentSortedSet(Comparator<Map.Entry<byte[],Document>> comparator, SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultByteDocumentComparator() : comparator), new ByteDocumentFileHandler(handler),
                        new FileByteDocumentSortedSet.Factory(), persisted);
    }

    /**
     * Create a persisted sorted set
     *
     * @param comparator
     *            the key comparator
     * @param rewriteStrategy
     *            the rewrite strategy
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileByteDocumentSortedSet(Comparator<Map.Entry<byte[],Document>> comparator, RewriteStrategy<Map.Entry<byte[],Document>> rewriteStrategy,
                    SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultByteDocumentComparator() : comparator), rewriteStrategy, new ByteDocumentFileHandler(handler),
                        new FileByteDocumentSortedSet.Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     */
    public FileByteDocumentSortedSet(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler) {
        super(set, new ByteDocumentFileHandler(handler), new FileByteDocumentSortedSet.Factory());
    }

    /**
     * Create an sorted set out of another sorted set. If persist is true, then the set will be directly persisted using the set's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     * @param persist
     *            boolean flag for persist
     * @throws IOException
     *             for issues with read/write
     */
    public FileByteDocumentSortedSet(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, new ByteDocumentFileHandler(handler), new FileByteDocumentSortedSet.Factory(), persist);
    }

    /**
     * This will dump the set to the file, making the set "persisted"
     *
     * @param handler
     *            the sorted set file handler
     * @throws IOException
     *             for issues with read/write
     */
    public void persist(SortedSetFileHandler handler) throws IOException {
        // ensure this handler is wrapped with our handler
        super.persist(new ByteDocumentFileHandler(handler));
    }

    /**
     * Clone this set
     */
    @Override
    public FileByteDocumentSortedSet clone() {
        return (FileByteDocumentSortedSet) super.clone();
    }

    /**
     * A sortedsetfilehandler that can bound the input stream
     */
    public static class ByteDocumentFileHandler implements BoundedTypedSortedSetFileHandler<Map.Entry<byte[],Document>> {
        SortedSetFileHandler delegate;

        public ByteDocumentFileHandler(SortedSetFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedSetInputStream<Map.Entry<byte[],Document>> getInputStream() throws IOException {
            return new RFileByteDocumentInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedSetInputStream<Map.Entry<byte[],Document>> getInputStream(Map.Entry<byte[],Document> start, Map.Entry<byte[],Document> end)
                        throws IOException {
            return new RFileByteDocumentInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedSetOutputStream getOutputStream() throws IOException {
            return new RFileByteDocumentOutputStream(delegate.getOutputStream());
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

    /**
     * A factory for these file sorted sets
     */
    public static class Factory implements FileSortedSetFactory<Map.Entry<byte[],Document>> {

        @Override
        public FileByteDocumentSortedSet newInstance(FileSortedSet<Map.Entry<byte[],Document>> other) {
            return new FileByteDocumentSortedSet((FileByteDocumentSortedSet) other);
        }

        @Override
        public FileByteDocumentSortedSet newInstance(FileSortedSet<Map.Entry<byte[],Document>> other, Map.Entry<byte[],Document> from,
                        Map.Entry<byte[],Document> to) {
            return new FileByteDocumentSortedSet((FileByteDocumentSortedSet) other, from, to);
        }

        @Override
        public FileByteDocumentSortedSet newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileByteDocumentSortedSet(handler, persisted);
        }

        @Override
        public FileByteDocumentSortedSet newInstance(Comparator<Map.Entry<byte[],Document>> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileByteDocumentSortedSet(comparator, handler, persisted);
        }

        @Override
        public FileSortedSet<Map.Entry<byte[],Document>> newInstance(Comparator<Map.Entry<byte[],Document>> comparator,
                        RewriteStrategy<Map.Entry<byte[],Document>> rewriteStrategy, SortedSetFileHandler handler, boolean persisted) {
            return new FileByteDocumentSortedSet(comparator, rewriteStrategy, handler, persisted);
        }

        @Override
        public FileByteDocumentSortedSet newInstance(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler) {
            return new FileByteDocumentSortedSet(set, handler);
        }

        @Override
        public FileByteDocumentSortedSet newInstance(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler, boolean persist)
                        throws IOException {
            return new FileByteDocumentSortedSet(set, handler, persist);
        }
    }
}
