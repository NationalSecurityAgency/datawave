package datawave.query.util.sortedmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSortedMapTest {
    SortedMap<Integer,Integer> data = null;
    SortedMap<Integer,Integer> extraData = null;
    FileSortedMap<Integer,Integer> map = null;
    datawave.query.util.sortedmap.SortedMapTempFileHandler handler = null;

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
        handler = new SortedMapTempFileHandler();
        map = new FileSerializableSortedMap<>(c, new FileSerializableSortedMap.SerializableFileHandler(handler), false);
        data = new TreeMap<>(c);
        Random r = new Random(123948710248L);
        // data.add(null);
        for (int i = 0; i < 20; i++) {
            data.put(r.nextInt(), r.nextInt() + 1);
        }
        extraData = new TreeMap<>(c);
        for (int i = 0; i < 20; i++) {
            extraData.put(r.nextInt(), r.nextInt() + 1);
        }
        // make sure we have no overlap
        data.keySet().removeAll(extraData.keySet());
        map.putAll(data);
    }

    @After
    public void tearDown() {
        handler.getFile().delete();
    }

    private void assertSortedMapEquals(SortedMap<Integer,Integer> map1, SortedMap<Integer,Integer> map2) {
        assertEquals(map1.size(), map2.size());
        assertTrue(map1.keySet().containsAll(map2.keySet()));
        assertTrue(map1.keySet().containsAll(map2.keySet()));
        map1.entrySet().stream().forEach(e -> assertEquals(e.getValue(), map2.get(e.getKey())));
    }

    @Test
    public void testReadWrite() throws Exception {
        assertFalse(map.isPersisted());
        assertSortedMapEquals(data, map);
        map.persist();
        assertTrue(map.isPersisted());
        assertTrue(handler.getFile().exists());
        assertSortedMapEquals(data, map);
        map.load();
        assertFalse(map.isPersisted());
        assertSortedMapEquals(data, map);
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertFalse(map.isEmpty());
        map.persist();
        assertFalse(map.isEmpty());
        map.clear();
        assertTrue(map.isEmpty());
        map.load();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testContains() throws Exception {
        SortedMap<Integer,Integer> someData = new TreeMap<>(data);
        someData.put(extraData.firstKey(), extraData.get(extraData.firstKey()));
        for (Integer i : data.keySet()) {
            assertTrue(map.containsKey(i));
        }
        for (Integer i : extraData.keySet()) {
            assertFalse(map.containsKey(i));
        }
        map.persist();
        for (Integer i : data.keySet()) {
            assertTrue(map.containsKey(i));
        }
        for (Integer i : extraData.keySet()) {
            assertFalse(map.containsKey(i));
        }
        map.load();
        for (Integer i : data.keySet()) {
            assertTrue(map.containsKey(i));
        }
        for (Integer i : extraData.keySet()) {
            assertFalse(map.containsKey(i));
        }
    }

    private void printMap(SortedMap<Integer,Integer> map1, SortedMap<Integer,Integer> map2) {
        Iterator<Map.Entry<Integer,Integer>> it1 = map1.entrySet().iterator();
        Iterator<Map.Entry<Integer,Integer>> it2 = map2.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<Integer,Integer> o1 = it1.next();
            Map.Entry<Integer,Integer> o2 = it2.next();
            System.out.println(o1 + " vs " + o2);
        }
        while (it1.hasNext()) {
            Map.Entry<Integer,Integer> o1 = it1.next();
            System.out.println(o1 + " vs (null)");
        }
        while (it2.hasNext()) {
            Map.Entry<Integer,Integer> o2 = it2.next();
            System.out.println("(null) vs " + o2);
        }
    }

    @Test
    public void testIterator() throws Exception {
        SortedMap<Integer,Integer> testData = new TreeMap<>(data);
        Iterator<Map.Entry<Integer,Integer>> dataIterator = testData.entrySet().iterator();
        Iterator<Map.Entry<Integer,Integer>> mapIterator = map.entrySet().iterator();
        while (dataIterator.hasNext()) {
            assertTrue(mapIterator.hasNext());
            assertEquals(dataIterator.next(), mapIterator.next());
        }
        assertFalse(mapIterator.hasNext());
        for (Map.Entry<Integer,Integer> i : map.entrySet()) {
            assertEquals(testData.remove(i.getKey()), i.getValue());
        }
        assertTrue(testData.isEmpty());
        map.persist();
        dataIterator = data.entrySet().iterator();
        mapIterator = map.entrySet().iterator();
        while (dataIterator.hasNext()) {
            assertTrue(mapIterator.hasNext());
            assertEquals(dataIterator.next(), mapIterator.next());
        }
        assertFalse(mapIterator.hasNext());
        testData.putAll(data);
        for (Map.Entry<Integer,Integer> i : map.entrySet()) {
            assertEquals(testData.remove(i.getKey()), i.getValue());
        }
        assertTrue(testData.isEmpty());
        map.load();
        dataIterator = data.entrySet().iterator();
        mapIterator = map.entrySet().iterator();
        while (dataIterator.hasNext()) {
            assertTrue(mapIterator.hasNext());
            assertEquals(dataIterator.next(), mapIterator.next());
        }
        assertFalse(mapIterator.hasNext());
        testData.putAll(data);
        for (Map.Entry<Integer,Integer> i : map.entrySet()) {
            assertEquals(testData.remove(i.getKey()), i.getValue());
        }
        assertTrue(testData.isEmpty());
    }

    @Test
    public void testPut() throws Exception {
        assertNull(map.put(extraData.firstKey(), extraData.get(extraData.firstKey())));
        assertEquals(data.size() + 1, map.size());
        assertTrue(map.containsKey(extraData.firstKey()));
        assertEquals(map.remove(extraData.firstKey()), extraData.get(extraData.firstKey()));
        assertEquals(data.size(), map.size());
        assertFalse(map.containsKey(extraData.firstKey()));
        map.persist();
        try {
            map.put(extraData.firstKey(), extraData.get(extraData.firstKey()));
            fail("Expected persisted map.add to fail");
        } catch (Exception e) {
            // expected
        }
        map.load();
        assertEquals(data.size(), map.size());
        assertFalse(map.containsKey(extraData.firstKey()));
    }

    @Test
    public void testPutAll() throws Exception {
        map.putAll(extraData);
        assertEquals(data.size() + extraData.size(), map.size());
        assertTrue(map.entrySet().containsAll(extraData.entrySet()));
        assertTrue(map.keySet().removeAll(extraData.keySet()));
        assertEquals(data.size(), map.size());
        assertFalse(map.containsKey(extraData.firstKey()));
        map.persist();
        try {
            map.putAll(extraData);
            fail("Expected persisted map.addAll to fail");
        } catch (Exception e) {
            // expected
        }
        map.load();
        assertEquals(data.size(), map.size());
        assertFalse(map.containsKey(extraData.firstKey()));
    }

    @Test
    public void testRetainAll() throws Exception {
        SortedMap<Integer,Integer> someData = new TreeMap<>(data);
        someData.remove(data.firstKey());
        someData.remove(data.lastKey());
        someData.put(extraData.firstKey(), extraData.get(extraData.firstKey()));
        someData.put(extraData.lastKey(), extraData.get(extraData.lastKey()));
        assertFalse(map.keySet().retainAll(data.keySet()));
        assertEquals(someData.size(), map.size());
        assertTrue(map.keySet().retainAll(someData.keySet()));
        assertEquals(data.size() - 2, map.size());
        assertFalse(map.keySet().containsAll(data.keySet()));
        assertFalse(map.keySet().containsAll(someData.keySet()));
        assertFalse(map.containsKey(data.lastKey()));
        assertTrue(map.keySet().retainAll(extraData.keySet()));
        assertTrue(map.isEmpty());

        map.putAll(data);
        map.persist();
        try {
            map.keySet().retainAll(someData.keySet());
            fail("Expected persisted map.retainAll to fail");
        } catch (Exception e) {
            // expected
        }

        map.load();
        assertEquals(data.size(), map.size());
        assertTrue(map.keySet().containsAll(data.keySet()));
    }

    @Test
    public void testRemoveAll() throws Exception {
        SortedMap<Integer,Integer> someData = new TreeMap<>(data);
        someData.remove(data.firstKey());
        someData.remove(data.lastKey());
        someData.put(extraData.firstKey(), extraData.get(extraData.firstKey()));
        someData.put(extraData.lastKey(), extraData.get(extraData.lastKey()));
        assertFalse(map.keySet().removeAll(extraData.keySet()));
        assertEquals(someData.size(), map.size());
        assertTrue(map.keySet().removeAll(someData.keySet()));
        assertEquals(2, map.size());
        assertFalse(map.keySet().containsAll(data.keySet()));
        assertFalse(map.keySet().containsAll(someData.keySet()));
        assertTrue(map.keySet().contains(data.firstKey()));
        assertTrue(map.keySet().contains(data.lastKey()));
        assertTrue(map.keySet().removeAll(data.keySet()));
        assertTrue(map.isEmpty());

        map.putAll(data);
        map.persist();
        try {
            map.keySet().removeAll(someData.keySet());
            fail("Expected persisted map.retainAll to fail");
        } catch (Exception e) {
            // expected
        }

        map.load();
        assertEquals(data.size(), map.size());
        assertTrue(map.keySet().containsAll(data.keySet()));
    }

    @Test
    public void testClear() throws Exception {
        map.clear();
        assertTrue(map.isEmpty());
        map.putAll(data);
        map.persist();
        map.clear();
        assertTrue(map.isEmpty());
        map.load();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testNoComparator() throws Exception {
        assertNotNull(map.comparator());
        map.persist();
        assertNotNull(map.comparator());
        map.load();
        assertNotNull(map.comparator());
        SortedMap<Integer,Integer> tempData = new TreeMap<>();
        tempData.putAll(data);

        map = new FileSerializableSortedMap<>(tempData, new FileSerializableSortedMap.SerializableFileHandler(handler), false);

        assertNull(map.comparator());
        assertSortedMapEquals(tempData, map);
        for (Integer i : map.keySet()) {
            assertEquals(tempData.firstKey(), i);
            tempData.remove(tempData.firstKey());
        }
        tempData.putAll(data);
        assertSortedMapEquals(tempData, map);
        map.persist();
        assertNull(map.comparator());
        map.load();
        assertNull(map.comparator());

        for (Integer i : map.keySet()) {
            assertEquals(tempData.firstKey(), i);
            tempData.remove(tempData.firstKey());
        }
    }

    @Test
    public void testSubmap() throws Exception {
        Integer fromElement = null;
        Integer toElement = null;
        int index = 0;
        for (Integer i : data.keySet()) {
            if (index == (data.size() / 3)) {
                fromElement = i;
            } else if (index == data.size() * 2 / 3) {
                toElement = i;
                break;
            }
            index++;
        }
        SortedMap<Integer,Integer> submap = map.subMap(fromElement, toElement);
        assertSortedMapEquals(data.subMap(fromElement, toElement), submap);
        map.persist();
        map.subMap(fromElement, toElement);
        assertSortedMapEquals(data.subMap(fromElement, toElement), submap);
        map.load();
        submap = map.subMap(fromElement, toElement);
        assertSortedMapEquals(data.subMap(fromElement, toElement), submap);
    }

    @Test
    public void testHeadmap() throws Exception {
        Integer toElement = null;
        int index = 0;
        for (Integer i : data.keySet()) {
            if (index == data.size() * 2 / 3) {
                toElement = i;
                break;
            }
            index++;
        }
        SortedMap<Integer,Integer> submap = map.headMap(toElement);
        assertSortedMapEquals(data.headMap(toElement), submap);
        map.persist();
        map.headMap(toElement);
        assertSortedMapEquals(data.headMap(toElement), submap);
        map.load();
        submap = map.headMap(toElement);
        assertSortedMapEquals(data.headMap(toElement), submap);
    }

    @Test
    public void testTailmap() throws Exception {
        Integer fromElement = null;
        int index = 0;
        for (Integer i : data.keySet()) {
            if (index == (data.size() / 3)) {
                fromElement = i;
                break;
            }
            index++;
        }
        SortedMap<Integer,Integer> submap = map.tailMap(fromElement);
        assertSortedMapEquals(data.tailMap(fromElement), submap);
        map.persist();
        map.tailMap(fromElement);
        assertSortedMapEquals(data.tailMap(fromElement), submap);
        map.load();
        submap = map.tailMap(fromElement);
        assertSortedMapEquals(data.tailMap(fromElement), submap);
    }

    @Test
    public void testFirstKey() throws Exception {
        assertEquals(data.firstKey(), map.firstKey());
        map.persist();
        assertEquals(data.firstKey(), map.firstKey());
        map.load();
        assertEquals(data.firstKey(), map.firstKey());
    }

    @Test
    public void testLast() throws Exception {
        assertEquals(data.lastKey(), map.lastKey());
        map.persist();
        assertEquals(data.lastKey(), map.lastKey());
        map.load();
        assertEquals(data.lastKey(), map.lastKey());
    }
}
