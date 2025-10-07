package datawave.query.util.sortedset;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * This is an sorted set of byte arrays which keeps one large byte array as the backing store and a separate array of indices and sizes in sorted value order.
 * The reason for building this sorted set structure is to minimize memory usage and object creation while maintaining fast add capabilities.
 *
 */
public class SortedByteSetBuffer extends AbstractSet<byte[]> implements SortedSet<byte[]> {
    public static final int AVERAGE_VALUE_SIZE = 32;
    public static final int DEFAULT_BUFFER_SIZE = 64;

    protected byte[] data = null;
    protected int[] sortedDataIndicies = null;
    protected byte[] sortedDataSizes = null;
    protected int size = 0;
    protected int bufferSize = 0;
    protected int modCount = 0;

    public SortedByteSetBuffer() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public SortedByteSetBuffer(int capacity) {
        this.data = new byte[capacity * AVERAGE_VALUE_SIZE];
        this.sortedDataIndicies = new int[capacity];
        this.sortedDataSizes = new byte[capacity];
    }

    /************************** Overridden methods *************************/

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof byte[]) {
            return binarySearch((byte[]) o) >= 0;
        }
        return false;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new SortedByteSetBufferIterator();
    }

    @Override
    public boolean add(byte[] e) {
        if (e.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("SortedByteSetBuffer does not support data elements greater than " + Byte.MAX_VALUE + " bytes");
        }
        int index = binarySearch(e);
        if (index < 0) {
            add(-1 - index, e);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        if (!(o instanceof byte[])) {
            return false;
        }
        int index = binarySearch((byte[]) o);
        if (index >= 0) {
            remove(index);
            return true;
        }
        return false;
    }

    @Override
    public void clear() {
        modCount++;
        size = 0;
        bufferSize = 0;
    }

    @Override
    public Comparator<? super byte[]> comparator() {
        return new ByteArrayComparator();
    }

    @Override
    public SortedSet<byte[]> subSet(byte[] fromElement, byte[] toElement) {
        return new SortedByteSubSetBuffer(fromElement, toElement);
    }

    @Override
    public SortedSet<byte[]> headSet(byte[] toElement) {
        return new SortedByteSubSetBuffer(null, toElement);
    }

    @Override
    public SortedSet<byte[]> tailSet(byte[] fromElement) {
        return new SortedByteSubSetBuffer(fromElement, null);
    }

    @Override
    public byte[] first() {
        if (size == 0) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_FIRST_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        }
        return get(0);
    }

    @Override
    public byte[] last() {
        if (size == 0) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_LAST_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        }
        return get(size - 1);
    }

    /* Other public methods */

    public byte[] get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index is out of range");
        }
        int dataIndex = sortedDataIndicies[index];
        int dataSize = sortedDataSizes[index];
        byte[] term = new byte[dataSize];
        System.arraycopy(data, dataIndex, term, 0, dataSize);
        return term;
    }

    /* The protected stuff */

    protected void checkCapacity(int plusSize) {
        int plusLen = 1;
        int minCapacity = bufferSize + plusSize;
        int oldCapacity = data.length;
        if (minCapacity > oldCapacity) {
            int newCapacity = (oldCapacity * 3) / 2 + 1;
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity;
            }
            plusLen = ((newCapacity - oldCapacity) / 32) + 1;
            data = Arrays.copyOf(data, newCapacity);
        }
        int minLen = size + plusLen;
        int oldLen = sortedDataIndicies.length;
        if (minLen > oldLen) {
            int newLen = (oldLen * 3) / 2 + 1;
            if (newLen < minLen) {
                newLen = minLen;
            }
            sortedDataIndicies = Arrays.copyOf(sortedDataIndicies, newLen);
            sortedDataSizes = Arrays.copyOf(sortedDataSizes, newLen);
        }
    }

    protected void add(int index, byte[] value) {
        modCount++;
        checkCapacity(value.length);
        int dataIndex = bufferSize;
        System.arraycopy(value, 0, data, bufferSize, value.length);
        System.arraycopy(sortedDataIndicies, index, sortedDataIndicies, index + 1, size - index);
        System.arraycopy(sortedDataSizes, index, sortedDataSizes, index + 1, size - index);
        sortedDataIndicies[index] = dataIndex;
        sortedDataSizes[index] = (byte) (value.length);
        bufferSize += value.length;
        size++;
    }

    protected void remove(int index) {
        modCount++;
        int dataIndex = sortedDataIndicies[index];
        int dataSize = sortedDataSizes[index];
        bufferSize -= dataSize;
        size--;
        System.arraycopy(data, dataIndex + dataSize, data, dataIndex, bufferSize - dataIndex);
        System.arraycopy(sortedDataSizes, index + 1, sortedDataSizes, index, size - index);
        System.arraycopy(sortedDataIndicies, index + 1, sortedDataIndicies, index, size - index);
        for (int i = 0; i < size; i++) {
            if (sortedDataIndicies[i] > dataIndex) {
                sortedDataIndicies[i] -= dataSize;
            }
        }
    }

    protected static int compare(byte[] data, int dataIndex, int dataSize, byte[] term) {
        int minSize = dataSize;
        if (term.length < minSize)
            minSize = term.length;
        int comparison = 0;
        for (int i = 0; i < minSize; i++) {
            comparison = data[dataIndex + i] - term[i];
            if (comparison != 0)
                break;
        }
        if (comparison == 0) {
            if (minSize < dataSize) {
                comparison = 1;
            } else if (minSize < term.length) {
                comparison = -1;
            }
        }
        return comparison;
    }

    /**
     * A binary search of the byte array based on a sorted index array
     *
     * @param term
     *            aterm
     * @return location result of the search
     */
    protected int binarySearch(byte[] term) {
        return binarySearch(term, 0, this.size - 1);
    }

    protected int binarySearch(byte[] term, int start, int end) {
        while (start <= end) {
            int middle = (start + end) >>> 1;
            int comparison = compare(data, sortedDataIndicies[middle], sortedDataSizes[middle], term);

            if (comparison < 0)
                start = middle + 1;
            else if (comparison > 0)
                end = middle - 1;
            else
                return middle;
        }
        // return a negative index if not found so we know where it should go
        return -(start + 1);
    }

    protected class SortedByteSetBufferIterator implements Iterator<byte[]> {
        protected int index = 0;
        protected int end = 0;
        protected int expectedModCount = -1;
        protected int last = -1;

        public SortedByteSetBufferIterator() {
            this(0, size);
        }

        public SortedByteSetBufferIterator(int start, int end) {
            this.expectedModCount = modCount;
            this.index = start;
            this.end = end;
        }

        final void checkModCount() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
        }

        @Override
        public boolean hasNext() {
            checkModCount();
            return index < end;
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            checkModCount();
            last = index;
            byte[] entry = get(index++);
            checkModCount();
            return entry;
        }

        @Override
        public void remove() {
            checkModCount();
            if (last >= 0) {
                SortedByteSetBuffer.this.remove(last);
                index--;
                end--;
                expectedModCount = modCount;
            } else {
                throw new IllegalStateException("next needs to be called first");
            }
        }
    }

    protected class SortedByteSubSetBuffer extends AbstractSet<byte[]> implements SortedSet<byte[]> {
        protected byte[] from;
        protected byte[] to;
        protected int expectedModCount = -1;
        protected int[] range = null;

        public SortedByteSubSetBuffer(byte[] from, byte[] to) {
            if (from != null && to != null && comparator().compare(from, to) > 0) {
                throw new IllegalArgumentException("The start is greater than the end");
            }
            this.from = from;
            this.to = to;
        }

        @Override
        public Comparator<? super byte[]> comparator() {
            return SortedByteSetBuffer.this.comparator();
        }

        @Override
        public SortedSet<byte[]> subSet(byte[] fromElement, byte[] toElement) {
            if ((from != null && comparator().compare(fromElement, from) < 0) || (to != null && comparator().compare(to, toElement) < 0)) {
                throw new IllegalArgumentException("Cannot create subset outside of the range of this subset");
            }
            return SortedByteSetBuffer.this.subSet(fromElement, toElement);
        }

        @Override
        public SortedSet<byte[]> headSet(byte[] toElement) {
            return subSet(from, toElement);
        }

        @Override
        public SortedSet<byte[]> tailSet(byte[] fromElement) {
            return subSet(fromElement, to);
        }

        @Override
        public byte[] first() {
            int[] range = getRange();
            if (range == null) {
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_FIRST_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            return get(range[0]);
        }

        @Override
        public byte[] last() {
            int[] range = getRange();
            if (range == null) {
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_LAST_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            return get(range[1]);
        }

        @Override
        public Iterator<byte[]> iterator() {
            int[] range = getRange();
            if (range == null) {
                return new SortedByteSetBufferIterator(0, 0);
            } else {
                return new SortedByteSetBufferIterator(range[0], range[1]);
            }
        }

        @Override
        public int size() {
            int[] range = getRange();
            if (range == null) {
                return 0;
            } else {
                return range[1] - range[0] + 1;
            }
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof byte[])) {
                return false;
            }
            int[] range = getRange();
            if (range == null) {
                return false;
            }
            boolean contains = (binarySearch((byte[]) o, range[0], range[1]) >= 0);
            checkModCount();
            return contains;
        }

        @Override
        public boolean add(byte[] e) {
            if ((from != null && comparator().compare(e, from) < 0) || (to != null && comparator().compare(e, to) >= 0)) {
                throw new IllegalArgumentException("Cannot add element outside of subset range");
            }
            return SortedByteSetBuffer.this.add(e);
        }

        @Override
        public boolean remove(Object o) {
            if (contains(o)) {
                return SortedByteSetBuffer.this.remove(o);
            }
            return false;
        }

        /***
         * Get the range of elements in the SortedByteSetBuffer
         *
         * @return int[] {firstIndex, lastIndex}
         */
        protected int[] getRange() {
            if (expectedModCount != modCount) {
                expectedModCount = modCount;
                if (SortedByteSetBuffer.this.isEmpty()) {
                    range = null;
                } else {

                    // find the first entry
                    int start = (from == null ? 0 : binarySearch(from));
                    if (start < 0) {
                        start = -1 - start;
                    }

                    // if the start is past the end, then we have no range
                    if (start == SortedByteSetBuffer.this.size()) {
                        range = null;
                    } else {

                        // find the last entry
                        int end = (to == null ? SortedByteSetBuffer.this.size() : binarySearch(to));
                        if (end < 0) {
                            end = -1 - end;
                        }
                        // since the end is exclusive, go to the previous element
                        end--;

                        // if the start is not before the end, then no range
                        if (start >= end) {
                            range = null;
                        } else {
                            range = new int[] {start, end};
                        }
                    }
                }
                checkModCount();
            }
            return range;
        }

        final void checkModCount() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
        }

    }

}
