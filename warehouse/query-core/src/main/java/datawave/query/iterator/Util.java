package datawave.query.iterator;

import java.util.Comparator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Document;

/**
 * Provides utility methods for text matching and multimaps that need to be referenced by the iterators.
 *
 */
@SuppressWarnings("rawtypes")
public class Util {
    public interface Transformer<T> {
        T transform(T t);
    }

    private static final Transformer<?> keyTransformer;

    private static final Comparator<Comparable> comparableComparator;

    private static final Comparator<?> hashComparator;

    private static final Comparator<?> nestedIteratorComparator;

    private static final TreeMultimap EMPTY;

    static {
        keyTransformer = (Transformer<Object>) o -> {
            if (o instanceof Key) {
                // for keys we only want to use the row and column family for comparison
                return new Key(((Key) o).getRow(), ((Key) o).getColumnFamily());
            }
            return o;
        };

        comparableComparator = (o1, o2) -> {
            if (o1 instanceof Key) {
                return ((Key) o1).compareTo((Key) o2, PartialKey.ROW_COLFAM);
            }
            return o1.compareTo(o2);
        };

        hashComparator = (Comparator<Object>) Comparator.comparingInt(Object::hashCode);

        nestedIteratorComparator = (Comparator<NestedIterator>) (o1, o2) -> {

            // reversed order to sort bigger documents first
            Document doc1 = o1.document();
            Document doc2 = o2.document();

            if (o1 == o2) {
                return 0;
            } else if (doc1 == null && doc2 == null) {
                return o2.hashCode() - o1.hashCode();
            } else if (doc2 == null) {
                return -1;
            } else if (doc1 == null) {
                return 1;
            } else {
                if (o2.document().compareTo(o1.document()) == 0) {
                    return o2.hashCode() - o1.hashCode();
                } else {
                    return o2.document().compareTo(o1.document());
                }
            }
        };

        EMPTY = TreeMultimap.create();
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable> Transformer<T> keyTransformer() {
        return (Transformer<T>) keyTransformer;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable> Comparator<T> keyComparator() {
        return (Comparator<T>) comparableComparator;
    }

    @SuppressWarnings("unchecked")
    public static <T> Comparator<T> hashComparator() {
        return (Comparator<T>) hashComparator;
    }

    @SuppressWarnings("unchecked")
    public static <T> Comparator<T> nestedIteratorComparator() {
        return (Comparator<T>) nestedIteratorComparator;
    }

    @SuppressWarnings("unchecked")
    public static <K,V> TreeMultimap<K,V> getEmpty() {
        return (TreeMultimap<K,V>) EMPTY;
    }

    public static Text minPrefix(Text root) {
        return appendSuffix(root, (byte) 0x00);
    }

    public static Text maxPrefix(Text root) {
        return appendSuffix(root, (byte) 0x01);
    }

    public static Text appendText(Text root, Text suffix) {
        if (null == suffix || 0 == suffix.getLength()) {
            return root;
        }

        root.append(suffix.getBytes(), 0, suffix.getLength());
        return root;
    }

    public static Text appendSuffix(Text root, byte suffix) {
        byte[] bytes = new byte[root.getLength() + 1];
        System.arraycopy(root.getBytes(), 0, bytes, 0, root.getLength());
        bytes[bytes.length - 1] = suffix;
        return new Text(bytes);
    }

    public static int prefixDiff(Text prefix, Text text) {
        int textEnd = (prefix.getLength() > text.getLength()) ? text.getLength() : prefix.getLength();

        return WritableComparator.compareBytes(prefix.getBytes(), 0, prefix.getLength(), text.getBytes(), 0, textEnd);
    }

    public static boolean prefixMatches(Text prefix, Text text) {
        return prefixDiff(prefix, text) == 0;
    }

    public static <T> Document buildNewDocument(Iterable<? extends NestedIterator<T>> iterators) {
        Document d = new Document();
        for (NestedIterator<T> iterator : iterators) {
            d.putAll(iterator.document().getDictionary().entrySet().iterator(), false);
        }
        return d;
    }

    /**
     * For cases when you have a mix of includes and context includes within a junction
     *
     * @param includes
     *            iterators
     * @param contextIncludes
     *            iterators that require context to evaluate
     * @return A document
     */
    public static <T> Document buildNewDocument(TreeMultimap<T,NestedIterator<T>> includes, TreeMultimap<T,NestedIterator<T>> contextIncludes, T lowest) {
        Document d = new Document();
        if (includes != null) {
            for (NestedIterator<T> include : includes.get(lowest)) {
                d.putAll(include.document().getDictionary().entrySet().iterator(), false);
            }
        }

        if (contextIncludes != null) {
            // context includes may not map to the lowest provided key
            for (NestedIterator<T> contextInclude : contextIncludes.get(lowest)) {
                Document doc = contextInclude.document();
                if (doc != null) {
                    d.putAll(doc.getDictionary().entrySet().iterator(), false);
                }
            }
        }
        return d;
    }

}
