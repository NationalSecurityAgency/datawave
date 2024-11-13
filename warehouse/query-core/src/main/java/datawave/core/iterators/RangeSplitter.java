package datawave.core.iterators;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

/**
 * This class will take a range and split it into a set of N ranges that completely cover the original range with no overlapping. This is down by taking the
 * first element (row, cq, or cf) that is different and creating intermediate byte sequences that are evenly distributed. between the start and end values. The
 * only time N ranges is not returned is when all of the elements are equal in which case only one range (the original range) is returned.
 */
public class RangeSplitter implements List<Range> {

    private List<Range> delegate = null;

    /**
     * Create a range splitter
     *
     * @param range
     *            a range
     * @param numRanges
     *            the number of ranges
     */
    public RangeSplitter(Range range, int numRanges) {
        delegate = Collections.unmodifiableList(splitRange(range, numRanges));
    }

    /**
     * Split a range into N ranges.
     *
     * @param range
     *            a range
     * @param numRanges
     *            the number of ranges
     * @return A list of N ranges. 1 range is returned if no splitting is possible.
     */
    protected List<Range> splitRange(Range range, int numRanges) {
        List<Range> splitRanges = new ArrayList<>();

        List<ByteSequence> start = getStartKeyElements(range.getStartKey());
        List<ByteSequence> end = getEndKeyElements(start, range.getEndKey());

        // loop through the key elements until we find a difference between the start and end
        for (int i = 0; i < start.size(); i++) {

            // if we have a difference, then go to work
            ByteSequence x = start.get(i);
            ByteSequence y = end.get(i);
            if (!equivalentElements(x, y)) {

                // get two integers representing the start and end that can be divided into numRanges
                BigInteger[] integers = getIntegers(x, y, numRanges);
                BigInteger xInt = integers[0];
                BigInteger div = integers[2];

                // Get the values initialized for the first range
                Key startKey = range.getStartKey();
                byte[] cv = (startKey == null ? new byte[0] : startKey.getColumnVisibility().getBytes());
                long ts = (startKey == null ? 0 : range.getStartKey().getTimestamp());
                boolean startKeyInclusive = range.isStartKeyInclusive();
                boolean endKeyInclusive = false;

                // now create ranges using the bytes for the integers between xInt and yInt by step sizes of div
                for (int j = 0; j < numRanges; j++) {
                    // Get the values for the end of the range
                    ByteSequence endRange = y;
                    Key endKey = range.getEndKey();

                    if (j < (numRanges - 1)) {
                        // if not the last range, then calculate the intermediate end key
                        xInt = xInt.add(div);
                        byte[] bytes = xInt.toByteArray();
                        if (bytes[0] == 0) {
                            endRange = new ArrayByteSequence(bytes, 1, bytes.length - 1);
                        } else {
                            endRange = new ArrayByteSequence(bytes);
                        }
                        // using the start components on either side of the calculated end element to ensure we do not
                        // create an invalid range (where start > end)
                        endKey = getKey(start.subList(0, i), endRange, start.subList(i + 1, end.size()), cv, ts);
                    } else {
                        // if the last range, then use the ending inclusive flag
                        endKeyInclusive = range.isEndKeyInclusive();
                    }

                    // create the range
                    splitRanges.add(new Range(startKey, startKeyInclusive, endKey, endKeyInclusive));

                    // now setup for the next range
                    startKeyInclusive = true;
                    endKeyInclusive = false;
                    startKey = endKey;
                }
                break;
            }
        }

        if (splitRanges.isEmpty()) {
            splitRanges.add(range);
        }

        return splitRanges;
    }

    /*
     * Create a key given a set of ByteSequences split up between start, middel, and end
     */
    protected Key getKey(List<ByteSequence> start, ByteSequence middle, List<ByteSequence> end, byte[] cv, long ts) {
        ByteSequence row, cf, cq = null;
        switch (start.size()) {
            case 0:
                row = middle;
                cf = end.get(0);
                cq = end.get(1);
                break;
            case 1:
                row = start.get(0);
                cf = middle;
                cq = end.get(0);
                break;
            default:
                row = start.get(0);
                cf = start.get(1);
                cq = middle;
                break;
        }
        return new Key(row.toArray(), cf.toArray(), cq.toArray(), cv, ts);
    }

    /**
     * Calculate integers representing x and y that can be divided into numRanges.
     *
     * @param x
     *            the x sequence
     * @param y
     *            the y sequence
     * @param numRanges
     *            a numnber of ranges
     * @return An array of xInt, yInt, and div where div is the size of each range relative to xInt and yInt
     */
    protected BigInteger[] getIntegers(ByteSequence x, ByteSequence y, int numRanges) {
        // adding one leading 0 filled byte to ensure positive integers here
        int len = Math.max(x.length(), y.length()) + 1;

        byte[] xBytes = new byte[len];
        System.arraycopy(x.getBackingArray(), x.offset(), xBytes, 1, x.length());

        byte[] yBytes = new byte[len];
        System.arraycopy(y.getBackingArray(), y.offset(), yBytes, 1, y.length());

        BigInteger xInt = new BigInteger(xBytes);
        BigInteger yInt = new BigInteger(yBytes);

        BigInteger num = new BigInteger(String.valueOf(numRanges));
        BigInteger diff = yInt.subtract(xInt);
        while (diff.compareTo(num) < 0) {
            len++;
            xBytes = new byte[len];
            System.arraycopy(x.getBackingArray(), x.offset(), xBytes, 1, x.length());

            yBytes = new byte[len];
            System.arraycopy(y.getBackingArray(), y.offset(), yBytes, 1, y.length());

            xInt = new BigInteger(xBytes);
            yInt = new BigInteger(yBytes);

            diff = yInt.subtract(xInt);
        }
        BigInteger div = diff.divide(num);
        return new BigInteger[] {xInt, yInt, div};
    }

    /**
     * Are two byte sequences equivalent. They are equivalent iff the roots (ignoring trailing null bytes) are equal.
     *
     * @param start
     *            the start sequence
     * @param end
     *            the end sequence
     * @return true if equivalent
     */
    public static boolean equivalentElements(ByteSequence start, ByteSequence end) {
        int startLen = start.length();
        int endLen = end.length();
        int len = Math.max(startLen, endLen);
        for (int i = 0; i < len; i++) {
            byte x = (i < startLen ? start.byteAt(i) : 0);
            byte y = (i < endLen ? end.byteAt(i) : 0);
            if (x != y) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return a list of ByteSequence objects representing the row, cf, and cq in that order.
     *
     * @param key
     *            a key
     * @return the byte sequences
     */
    protected List<ByteSequence> getKeyElements(Key key) {
        List<ByteSequence> bytes = new ArrayList<>();
        bytes.add(key.getRowData());
        bytes.add(key.getColumnFamilyData());
        bytes.add(key.getColumnQualifierData());
        return bytes;
    }

    /**
     * Return a list of ByteSequence objects representing the row, cf, and cq in that order. If the key is null, then return 3 empty byte sequences.
     *
     * @param key
     *            a key
     * @return the byte sequences
     */
    protected List<ByteSequence> getStartKeyElements(Key key) {
        if (key != null) {
            return getKeyElements(key);
        } else {
            List<ByteSequence> bytes = new ArrayList<>();
            bytes.add(new ArrayByteSequence(new byte[1]));
            bytes.add(new ArrayByteSequence(new byte[1]));
            bytes.add(new ArrayByteSequence(new byte[1]));
            return bytes;
        }
    }

    /**
     * Return a list of ByteSequence objects representing the row, cf, and cq in that order. However since we are dealing with the end key, we need to fix up a
     * few things. For empty elements, return 3 byte sequences filled with 0xFF the same size as the start byte sequences. This will allow us to calculate
     * reasonable ranges ranges.
     *
     * @param key
     *            a key
     * @param startElements
     *            list of the start elements
     * @return the byte sequences
     */
    protected List<ByteSequence> getEndKeyElements(List<ByteSequence> startElements, Key key) {
        List<ByteSequence> bytes = new ArrayList<>(startElements.size());
        if (key != null) {
            List<ByteSequence> endElements = getKeyElements(key);
            for (int i = 0; i < endElements.size(); i++) {
                ByteSequence startBytes = startElements.get(i);
                ByteSequence endBytes = endElements.get(i);
                if (endBytes.length() == 0 && startBytes.length() > 0) {
                    byte[] end = new byte[startBytes.length()];
                    Arrays.fill(end, (byte) 0xFF);
                    bytes.add(new ArrayByteSequence(end));
                } else {
                    bytes.add(endBytes);
                }
            }
        } else {
            for (ByteSequence startBytes : startElements) {
                byte[] end = new byte[startBytes.length()];
                Arrays.fill(end, (byte) 0xFF);
                bytes.add(new ArrayByteSequence(end));
            }
        }
        return bytes;
    }

    public int size() {
        return delegate.size();
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    public Iterator<Range> iterator() {
        return delegate.iterator();
    }

    public Object[] toArray() {
        return delegate.toArray();
    }

    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    public boolean add(Range e) {
        return delegate.add(e);
    }

    public boolean remove(Object o) {
        return delegate.remove(o);
    }

    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    public boolean addAll(Collection<? extends Range> c) {
        return delegate.addAll(c);
    }

    public boolean addAll(int index, Collection<? extends Range> c) {
        return delegate.addAll(index, c);
    }

    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        return delegate.retainAll(c);
    }

    public void clear() {
        delegate.clear();
    }

    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public Range get(int index) {
        return delegate.get(index);
    }

    public Range set(int index, Range element) {
        return delegate.set(index, element);
    }

    public void add(int index, Range element) {
        delegate.add(index, element);
    }

    public Range remove(int index) {
        return delegate.remove(index);
    }

    public int indexOf(Object o) {
        return delegate.indexOf(o);
    }

    public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }

    public ListIterator<Range> listIterator() {
        return delegate.listIterator();
    }

    public ListIterator<Range> listIterator(int index) {
        return delegate.listIterator(index);
    }

    public List<Range> subList(int fromIndex, int toIndex) {
        return delegate.subList(fromIndex, toIndex);
    }

}
