package datawave.query.util.sortedset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSortedSetTest {
    SortedSet<Integer> data = null;
    SortedSet<Integer> extraData = null;
    FileSortedSet<Integer> set = null;
    SortedSetTempFileHandler handler = null;

    @Before
    public void setUp() throws Exception {
        Comparator<Integer> c = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if (o1 == null) {
                    return (o2 == null ? 0 : -1);
                } else {
                    return (o2 == null ? 1 : o1.compareTo(o2));
                }
            }
        };
        handler = new SortedSetTempFileHandler();
        set = new FileSerializableSortedSet<>(new TreeSet<>(c), new FileSerializableSortedSet.SerializableFileHandler(handler));
        data = new TreeSet<>(c);
        Random r = new Random(123948710248L);
        // data.add(null);
        for (int i = 0; i < 20; i++) {
            data.add(r.nextInt());
        }
        extraData = new TreeSet<>(c);
        for (int i = 0; i < 20; i++) {
            extraData.add(r.nextInt());
        }
        extraData.removeAll(data);
        set.addAll(data);
    }

    @After
    public void tearDown() {
        handler.getFile().delete();
    }

    @Test
    public void testReadWrite() throws Exception {
        assertFalse(set.isPersisted());
        assertEquals(data.size(), set.size());
        assertTrue(set.containsAll(data));
        assertTrue(data.containsAll(set));
        set.persist();
        assertTrue(set.isPersisted());
        assertTrue(handler.getFile().exists());
        assertEquals(data.size(), set.size());
        assertTrue(set.containsAll(data));
        assertTrue(data.containsAll(set));
        set.load();
        assertFalse(set.isPersisted());
        assertEquals(data.size(), set.size());
        assertTrue(set.containsAll(data));
        assertTrue(data.containsAll(set));
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertFalse(set.isEmpty());
        set.persist();
        assertFalse(set.isEmpty());
        set.clear();
        assertTrue(set.isEmpty());
        set.load();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testContains() throws Exception {
        SortedSet<Integer> someData = new TreeSet<>(data);
        someData.add(extraData.first());
        assertTrue(set.containsAll(data));
        for (Integer i : data) {
            assertTrue(set.contains(i));
        }
        assertFalse(set.containsAll(someData));
        for (Integer i : extraData) {
            assertFalse(set.contains(i));
        }
        set.persist();
        assertTrue(set.containsAll(data));
        for (Integer i : data) {
            assertTrue(set.contains(i));
        }
        assertFalse(set.containsAll(someData));
        for (Integer i : extraData) {
            assertFalse(set.contains(i));
        }
        set.load();
        assertTrue(set.containsAll(data));
        for (Integer i : data) {
            assertTrue(set.contains(i));
        }
        assertFalse(set.containsAll(someData));
        for (Integer i : extraData) {
            assertFalse(set.contains(i));
        }
    }

    @Test
    public void testIterator() throws Exception {
        SortedSet<Integer> testData = new TreeSet<>(data);
        Iterator<Integer> dataIterator = testData.iterator();
        Iterator<Integer> setIterator = set.iterator();
        while (dataIterator.hasNext()) {
            assertTrue(setIterator.hasNext());
            assertEquals(dataIterator.next(), setIterator.next());
        }
        assertFalse(setIterator.hasNext());
        for (Integer i : set) {
            assertTrue(testData.remove(i));
        }
        assertTrue(testData.isEmpty());
        set.persist();
        dataIterator = data.iterator();
        setIterator = set.iterator();
        while (dataIterator.hasNext()) {
            assertTrue(setIterator.hasNext());
            assertEquals(dataIterator.next(), setIterator.next());
        }
        assertFalse(setIterator.hasNext());
        testData.addAll(data);
        for (Integer i : set) {
            assertTrue(testData.remove(i));
        }
        assertTrue(testData.isEmpty());
        set.load();
        dataIterator = data.iterator();
        setIterator = set.iterator();
        while (dataIterator.hasNext()) {
            assertTrue(setIterator.hasNext());
            assertEquals(dataIterator.next(), setIterator.next());
        }
        assertFalse(setIterator.hasNext());
        testData.addAll(data);
        for (Integer i : set) {
            assertTrue(testData.remove(i));
        }
        assertTrue(testData.isEmpty());
    }

    @Test
    public void testToArray() throws Exception {
        Object[] a = set.toArray();
        Object[] d = data.toArray();
        assertArrayEquals(d, a);
        set.persist();
        a = set.toArray();
        assertArrayEquals(d, a);
        set.load();
        a = set.toArray();
        assertArrayEquals(d, a);
    }

    @Test
    public void testToArrayTArray() throws Exception {
        Integer[] d = data.toArray(new Integer[set.size()]);

        Integer[] a = set.toArray(new Integer[set.size()]);
        assertArrayEquals(d, a);
        set.persist();
        a = set.toArray(new Integer[set.size()]);
        assertArrayEquals(d, a);
        set.load();
        a = set.toArray(new Integer[set.size()]);
        assertArrayEquals(d, a);

        a = set.toArray(new Integer[set.size()]);
        assertArrayEquals(d, a);
        set.persist();
        a = set.toArray(new Integer[set.size()]);
        assertArrayEquals(d, a);
        set.load();
        a = set.toArray(new Integer[set.size()]);
        assertArrayEquals(d, a);

        d = data.toArray(new Integer[set.size() * 2]);
        a = set.toArray((new Integer[set.size() * 2]));
        assertArrayEquals(d, a);
        set.persist();
        a = set.toArray(new Integer[set.size() * 2]);
        assertArrayEquals(d, a);
        set.load();
        a = set.toArray(new Integer[set.size() * 2]);
        assertArrayEquals(d, a);
    }

    @Test
    public void testAdd() throws Exception {
        assertTrue(set.add(extraData.first()));
        assertEquals(data.size() + 1, set.size());
        assertTrue(set.contains(extraData.first()));
        assertTrue(set.remove(extraData.first()));
        assertEquals(data.size(), set.size());
        assertFalse(set.contains(extraData.first()));
        set.persist();
        try {
            set.add(extraData.first());
            fail("Expected persisted set.add to fail");
        } catch (Exception e) {
            // expected
        }
        set.load();
        assertEquals(data.size(), set.size());
        assertFalse(set.contains(extraData.first()));
    }

    @Test
    public void testAddAll() throws Exception {
        assertTrue(set.addAll(extraData));
        assertEquals(data.size() + extraData.size(), set.size());
        assertTrue(set.containsAll(extraData));
        assertTrue(set.removeAll(extraData));
        assertEquals(data.size(), set.size());
        assertFalse(set.contains(extraData.first()));
        set.persist();
        try {
            set.addAll(extraData);
            fail("Expected persisted set.addAll to fail");
        } catch (Exception e) {
            // expected
        }
        set.load();
        assertEquals(data.size(), set.size());
        assertFalse(set.contains(extraData.first()));
    }

    @Test
    public void testRetainAll() throws Exception {
        SortedSet<Integer> someData = new TreeSet<>(data);
        someData.remove(data.first());
        someData.remove(data.last());
        someData.add(extraData.first());
        someData.add(extraData.last());
        assertFalse(set.retainAll(data));
        assertEquals(someData.size(), set.size());
        assertTrue(set.retainAll(someData));
        assertEquals(data.size() - 2, set.size());
        assertFalse(set.containsAll(data));
        assertFalse(set.containsAll(someData));
        assertFalse(set.contains(data.last()));
        assertTrue(set.retainAll(extraData));
        assertTrue(set.isEmpty());

        set.addAll(data);
        set.persist();
        try {
            set.retainAll(someData);
            fail("Expected persisted set.retainAll to fail");
        } catch (Exception e) {
            // expected
        }

        set.load();
        assertEquals(data.size(), set.size());
        assertTrue(set.containsAll(data));
    }

    @Test
    public void testRemoveAll() throws Exception {
        SortedSet<Integer> someData = new TreeSet<>(data);
        someData.remove(data.first());
        someData.remove(data.last());
        someData.add(extraData.first());
        someData.add(extraData.last());
        assertFalse(set.removeAll(extraData));
        assertEquals(someData.size(), set.size());
        assertTrue(set.removeAll(someData));
        assertEquals(2, set.size());
        assertFalse(set.containsAll(data));
        assertFalse(set.containsAll(someData));
        assertTrue(set.contains(data.first()));
        assertTrue(set.contains(data.last()));
        assertTrue(set.removeAll(data));
        assertTrue(set.isEmpty());

        set.addAll(data);
        set.persist();
        try {
            set.removeAll(someData);
            fail("Expected persisted set.retainAll to fail");
        } catch (Exception e) {
            // expected
        }

        set.load();
        assertEquals(data.size(), set.size());
        assertTrue(set.containsAll(data));
    }

    @Test
    public void testClear() throws Exception {
        set.clear();
        assertTrue(set.isEmpty());
        set.addAll(data);
        set.persist();
        set.clear();
        assertTrue(set.isEmpty());
        set.load();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testNoComparator() throws Exception {
        assertNotNull(set.comparator());
        set.persist();
        assertNotNull(set.comparator());
        set.load();
        assertNotNull(set.comparator());
        SortedSet<Integer> tempData = new TreeSet<>();
        for (Integer i : data) {
            if (i != null) {
                tempData.add(i);
            }
        }

        set = new FileSerializableSortedSet<>(tempData, new FileSerializableSortedSet.SerializableFileHandler(handler));

        assertNull(set.comparator());
        assertEquals(tempData, set);
        for (Integer i : set) {
            assertEquals(tempData.first(), i);
            tempData.remove(tempData.first());
        }
        for (Integer i : data) {
            if (i != null) {
                tempData.add(i);
            }
        }
        assertEquals(tempData, set);
        set.persist();
        assertNull(set.comparator());
        set.load();
        assertNull(set.comparator());

        for (Integer i : data) {
            assertEquals(tempData.first(), i);
            tempData.remove(tempData.first());
        }
    }

    @Test
    public void testSubSet() throws Exception {
        Integer fromElement = null;
        Integer toElement = null;
        int index = 0;
        for (Integer i : data) {
            if (index == (data.size() / 3)) {
                fromElement = i;
            } else if (index == data.size() * 2 / 3) {
                toElement = i;
                break;
            }
            index++;
        }
        SortedSet<Integer> subSet = set.subSet(fromElement, toElement);
        assertEquals(data.subSet(fromElement, toElement), subSet);
        set.persist();
        set.subSet(fromElement, toElement);
        assertEquals(data.subSet(fromElement, toElement), subSet);
        set.load();
        subSet = set.subSet(fromElement, toElement);
        assertEquals(data.subSet(fromElement, toElement), subSet);
    }

    @Test
    public void testHeadSet() throws Exception {
        Integer toElement = null;
        int index = 0;
        for (Integer i : data) {
            if (index == data.size() * 2 / 3) {
                toElement = i;
                break;
            }
            index++;
        }
        SortedSet<Integer> subSet = set.headSet(toElement);
        assertEquals(data.headSet(toElement), subSet);
        set.persist();
        set.headSet(toElement);
        assertEquals(data.headSet(toElement), subSet);
        set.load();
        subSet = set.headSet(toElement);
        assertEquals(data.headSet(toElement), subSet);
    }

    @Test
    public void testTailSet() throws Exception {
        Integer fromElement = null;
        int index = 0;
        for (Integer i : data) {
            if (index == (data.size() / 3)) {
                fromElement = i;
                break;
            }
            index++;
        }
        SortedSet<Integer> subSet = set.tailSet(fromElement);
        assertEquals(data.tailSet(fromElement), subSet);
        set.persist();
        set.tailSet(fromElement);
        assertEquals(data.tailSet(fromElement), subSet);
        set.load();
        subSet = set.tailSet(fromElement);
        assertEquals(data.tailSet(fromElement), subSet);
    }

    @Test
    public void testFirst() throws Exception {
        assertEquals(data.first(), set.first());
        set.persist();
        assertEquals(data.first(), set.first());
        set.load();
        assertEquals(data.first(), set.first());
    }

    @Test
    public void testLast() throws Exception {
        assertEquals(data.last(), set.last());
        set.persist();
        assertEquals(data.last(), set.last());
        set.load();
        assertEquals(data.last(), set.last());
    }
}
