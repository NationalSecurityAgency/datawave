package datawave.query.util.sortedmap;

import datawave.query.util.sortedset.SortedByteSetBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SortedByteSetBufferTest {
    private byte[][] data = null;
    private int[] sortedOrder = null;
    private datawave.query.util.sortedset.SortedByteSetBuffer set = null;

    @Before
    public void setUp() {
        byte[] template = new byte[] {5, 2, 78, 4, 8, 3, 54, 23, 6, 21, 7, 16};
        int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};
        data = new byte[template.length * 2][];
        for (int i = 0; i < template.length; i++) {
            data[i] = new byte[i + 11];
            Arrays.fill(data[i], template[i]);
        }
        for (int i = 0; i < template.length; i++) {
            data[i + template.length] = new byte[10];
            Arrays.fill(data[i + template.length], template[i]);
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        set = new datawave.query.util.sortedset.SortedByteSetBuffer(5);
        Collections.addAll(set, data);
    }

    /**
     * @throws Exception
     */
    @After
    public void tearDown() {
        data = null;
        sortedOrder = null;
        set.clear();
        set = null;
    }

    /**
     * Test method for {@link datawave.query.util.sortedset.SortedByteSetBuffer#size()}.
     */
    @Test
    public void testSize() {
        int expectedSize = data.length;
        assertEquals(expectedSize, set.size());
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
            expectedSize--;
            assertEquals(expectedSize, set.size());
        }
        for (int i = 0; i < (data.length / 2); i++) {
            set.remove(data[i]);
            expectedSize--;
            assertEquals(expectedSize, set.size());
        }
        assertEquals(0, set.size());
        for (int i = 0; i < data.length; i++) {
            set.add(data[i]);
            expectedSize++;
            assertEquals(expectedSize, set.size());
        }
    }

    /**
     * Test method for {@link datawave.query.util.sortedset.SortedByteSetBuffer#isEmpty()}.
     */
    @Test
    public void testIsEmpty() {
        assertFalse(set.isEmpty());
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
            assertFalse(set.isEmpty());
        }
        for (int i = 1; i < (data.length / 2); i++) {
            set.remove(data[i]);
            assertFalse(set.isEmpty());
        }
        set.remove(data[0]);
        assertTrue(set.isEmpty());
        for (int i = 0; i < data.length; i++) {
            set.add(data[i]);
            assertFalse(set.isEmpty());
        }
    }

    /**
     * Test method for {@link datawave.query.util.sortedset.SortedByteSetBuffer#clear()}.
     */
    @Test
    public void testClear() {
        set.clear();
        assertTrue(set.isEmpty());
        for (int i = 0; i < data.length; i++) {
            set.add(data[i]);
            assertFalse(set.isEmpty());
        }
        set.clear();
        assertTrue(set.isEmpty());
    }

    /**
     * Test method for {@link datawave.query.util.sortedset.SortedByteSetBuffer#contains(Object)}.
     */
    @Test
    public void testContainsObject() {
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
        }
        for (int i = 0; i < (data.length / 2); i++) {
            assertTrue(set.contains(data[i]));
        }
        for (int i = (data.length / 2); i < data.length; i++) {
            assertFalse(set.contains(data[i]));
        }
    }

    /**
     * Test method for {@link datawave.query.util.sortedset.SortedByteSetBuffer#iterator()}.
     */
    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<byte[]> it = set.iterator(); it.hasNext();) {
            byte[] value = it.next();
            byte[] expected = data[sortedOrder[index++]];
            assertArrayEquals(expected, value);
        }
        set.clear();
        for (@SuppressWarnings("unused")
        byte[] value : set) {
            fail();
        }
    }

    /**
     * Test method fo {@link nsa.datawave.data.SortedByteSetBuffer#iterator().remove()}.
     */
    @Test
    public void testIteratorRemove() {
        int size = set.size();
        for (Iterator<byte[]> it = set.iterator(); it.hasNext();) {
            byte[] value = it.next();
            assertTrue(set.contains(value));
            it.remove();
            assertFalse(set.contains((value)));
            size--;
            assertEquals(size, set.size());
        }
        assertTrue(set.isEmpty());
    }

    /**
     * Test method for {@link SortedByteSetBuffer#comparator()}.
     */
    @Test
    public void testComparator() {
        Comparator<? super byte[]> comparator = set.comparator();
        byte[][] testData = Arrays.copyOf(data, data.length);
        Arrays.sort(testData, comparator);
        int index = 0;
        for (byte[] value : set) {
            byte[] expected = data[sortedOrder[index++]];
            assertArrayEquals(expected, value);
        }
    }

    /**
     * Test method for {@link SortedByteSetBuffer#subSet(byte[]. byte[]}/
     */
    @Test
    public void testSubSet() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        SortedSet<byte[]> subSet = set.subSet(data[sortedOrder[start]], data[sortedOrder[end]]);

        // verify contents
        assertEquals(end - start, subSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify order
        assertArrayEquals(data[sortedOrder[start]], subSet.first());
        int index = start;
        for (byte[] value : subSet) {
            assertArrayEquals(data[sortedOrder[index++]], value);
        }
        assertArrayEquals(data[sortedOrder[end - 1]], subSet.last());

        // verify add
        assertFalse(subSet.add(data[sortedOrder[start]]));
        assertFalse(subSet.add(data[sortedOrder[end - 1]]));
        try {
            subSet.add(data[sortedOrder[start - 1]]);
            fail("Expected to not be able to add something outside the range");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        try {
            subSet.add(data[sortedOrder[end]]);
            fail("Expected to not be able to add something outside the range");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        byte[] startValue = data[sortedOrder[start]];
        byte[] value = Arrays.copyOf(startValue, startValue.length + 50);
        assertTrue(subSet.add(value));
        assertEquals(end - start + 1, subSet.size());
        assertEquals(data.length + 1, set.size());
        assertTrue(subSet.contains(value));
        assertTrue(set.contains(value));
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify remove
        assertFalse(subSet.remove(data[sortedOrder[start - 1]]));
        assertFalse(subSet.remove(data[sortedOrder[end]]));
        assertTrue(subSet.remove(value));
        assertEquals(end - start, subSet.size());
        assertEquals(data.length, set.size());
        assertFalse(subSet.contains(value));
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify subSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start - 1]], data[sortedOrder[end]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start]], data[sortedOrder[end + 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start + 1]], data[sortedOrder[end - 1]]);
        assertEquals(end - start - 2, subSubSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i > start && i < (end - 1)) {
                assertTrue(subSubSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSubSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify tailSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subTailSet = subSet.tailSet(data[sortedOrder[start - 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subTailSet = subSet.tailSet(data[sortedOrder[start + 1]]);
        assertEquals(end - start - 1, subTailSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i > start && i < end) {
                assertTrue(subTailSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subTailSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify headSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subHeadSet = subSet.headSet(data[sortedOrder[end + 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subHeadSet = subSet.headSet(data[sortedOrder[end - 1]]);
        assertEquals(end - start - 1, subHeadSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < (end - 1)) {
                assertTrue(subHeadSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subHeadSet.contains(data[sortedOrder[i]]));
            }
        }
    }

    /**
     * Test method for {@link SortedByteSetBuffer#headSet(byte[])}
     */
    @Test
    public void testHeadSet() {
        int end = sortedOrder.length / 3;
        int start = 0;
        SortedSet<byte[]> subSet = set.headSet((data[sortedOrder[end]]));

        // verify contents
        assertEquals(end - start, subSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify order
        assertArrayEquals(data[sortedOrder[start]], subSet.first());
        int index = start;
        for (byte[] value : subSet) {
            assertArrayEquals(data[sortedOrder[index++]], value);
        }
        assertArrayEquals(data[sortedOrder[end - 1]], subSet.last());

        // verify add
        assertFalse(subSet.add(data[sortedOrder[start]]));
        assertFalse(subSet.add(data[sortedOrder[end - 1]]));
        try {
            subSet.add(data[sortedOrder[end]]);
            fail("Expected to not be able to add something outside the range");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        byte[] startValue = data[sortedOrder[start]];
        byte[] value = Arrays.copyOf(startValue, startValue.length + 50);
        assertTrue(subSet.add(value));
        assertEquals(end - start + 1, subSet.size());
        assertEquals(data.length + 1, set.size());
        assertTrue(subSet.contains(value));
        assertTrue(set.contains(value));
        for (int i = 0; i < data.length; i++) {
            if (i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify remove
        assertFalse(subSet.remove(data[sortedOrder[end]]));
        assertTrue(subSet.remove(value));
        assertEquals(end - start, subSet.size());
        assertEquals(data.length, set.size());
        assertFalse(subSet.contains(value));
        for (int i = 0; i < data.length; i++) {
            if (i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify subSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start]], data[sortedOrder[end + 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start + 1]], data[sortedOrder[end - 1]]);
        assertEquals(end - start - 2, subSubSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i > start && i < (end - 1)) {
                assertTrue(subSubSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSubSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify tailSet
        SortedSet<byte[]> subTailSet = subSet.tailSet(data[sortedOrder[start + 1]]);
        assertEquals(end - start - 1, subTailSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i > start && i < end) {
                assertTrue(subTailSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subTailSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify headSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subHeadSet = subSet.headSet(data[sortedOrder[end + 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subHeadSet = subSet.headSet(data[sortedOrder[end - 1]]);
        assertEquals(end - start - 1, subHeadSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i < (end - 1)) {
                assertTrue(subHeadSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subHeadSet.contains(data[sortedOrder[i]]));
            }
        }
    }

    /**
     * Test method for {@link SortedByteSetBuffer#tailSet(byte[])}.
     */
    @Test
    public void testTailSet() {
        int start = sortedOrder.length / 3;
        int end = sortedOrder.length;
        SortedSet<byte[]> subSet = set.tailSet(data[sortedOrder[start]]);

        // verify contents
        assertEquals(end - start, subSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify order
        assertArrayEquals(data[sortedOrder[start]], subSet.first());
        int index = start;
        for (byte[] value : subSet) {
            assertArrayEquals(data[sortedOrder[index++]], value);
        }
        assertArrayEquals(data[sortedOrder[end - 1]], subSet.last());

        // verify add
        assertFalse(subSet.add(data[sortedOrder[start]]));
        assertFalse(subSet.add(data[sortedOrder[end - 1]]));
        try {
            subSet.add(data[sortedOrder[start - 1]]);
            fail("Expected to not be able to add something outside the range");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        byte[] startValue = data[sortedOrder[start]];
        byte[] value = Arrays.copyOf(startValue, startValue.length + 50);
        assertTrue(subSet.add(value));
        assertEquals(end - start + 1, subSet.size());
        assertEquals(data.length + 1, set.size());
        assertTrue(subSet.contains(value));
        assertTrue(set.contains(value));
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify remove
        assertFalse(subSet.remove(data[sortedOrder[start - 1]]));
        assertTrue(subSet.remove(value));
        assertEquals(end - start, subSet.size());
        assertEquals(data.length, set.size());
        assertFalse(subSet.contains(value));
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < end) {
                assertTrue(subSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify subSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start - 1]], data[sortedOrder[end - 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subSubSet = subSet.subSet(data[sortedOrder[start + 1]], data[sortedOrder[end - 1]]);
        assertEquals(end - start - 2, subSubSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i > start && i < (end - 1)) {
                assertTrue(subSubSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subSubSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify tailSet
        try {
            @SuppressWarnings("unused")
            SortedSet<byte[]> subTailSet = subSet.tailSet(data[sortedOrder[start - 1]]);
            fail("Expected to not be able to create a supper set out of a sub set");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        SortedSet<byte[]> subTailSet = subSet.tailSet(data[sortedOrder[start + 1]]);
        assertEquals(end - start - 1, subTailSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i > start && i < end) {
                assertTrue(subTailSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subTailSet.contains(data[sortedOrder[i]]));
            }
        }

        // verify headSet
        SortedSet<byte[]> subHeadSet = subSet.headSet(data[sortedOrder[end - 1]]);
        assertEquals(end - start - 1, subHeadSet.size());
        for (int i = 0; i < data.length; i++) {
            if (i >= start && i < (end - 1)) {
                assertTrue(subHeadSet.contains(data[sortedOrder[i]]));
            } else {
                assertFalse(subHeadSet.contains(data[sortedOrder[i]]));
            }
        }
    }

    /**
     * Test method for {@link SortedByteSetBuffer#get(int)}.
     */
    @Test
    public void testGet() {
        for (int i = 0; i < data.length; i++) {
            byte[] expected = data[sortedOrder[i]];
            byte[] value = set.get(i);
            assertArrayEquals(expected, value);
        }
    }

    /**
     * Test method for {@link SortedByteSetBuffer#last()}.
     */
    @Test
    public void testLast() {
        byte[] expected = data[sortedOrder[data.length - 1]];
        byte[] value = set.last();
        assertArrayEquals(expected, value);
    }

    /**
     * Test method for {@link SortedByteSetBuffer#first()}.
     */
    @Test
    public void testFirst() {
        byte[] expected = data[sortedOrder[0]];
        byte[] value = set.first();
        assertArrayEquals(expected, value);
    }
}
