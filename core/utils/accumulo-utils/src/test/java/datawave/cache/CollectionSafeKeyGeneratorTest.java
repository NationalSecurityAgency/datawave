package datawave.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.springframework.cache.interceptor.SimpleKey;

/**
 * Test the CollectionSafeKeyGenerator class
 */
public class CollectionSafeKeyGeneratorTest {
    
    @Test
    public void testListCopy() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(list);
        assertNotSame(copy, list);
        assertEquals(copy, list);
    }
    
    @Test
    public void testSetCopy() {
        HashSet<Object> set = new HashSet<>();
        set.add(new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(set);
        assertNotSame(copy, set);
        assertEquals(copy, set);
    }
    
    @Test
    public void testSortedSetCopy() {
        TreeSet<String> set = new TreeSet<>();
        set.add("Hello World");
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(set);
        assertNotSame(copy, set);
        assertEquals(copy, set);
    }
    
    @Test
    public void testMapCopy() {
        HashMap<Object,Object> map = new HashMap<>();
        map.put(new Object(), new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(map);
        assertNotSame(copy, map);
        assertEquals(copy, map);
    }
    
    @Test
    public void testSortedMapCopy() {
        TreeMap<String,Object> map = new TreeMap<>();
        map.put("Hello World", new Object());
        Object copy = CollectionSafeKeyGenerator.copyIfCollectionParam(map);
        assertNotSame(copy, map);
        assertEquals(copy, map);
    }
    
    @Test
    public void testEmptyKey() {
        Object key = CollectionSafeKeyGenerator.generateKey();
        assertSame(key, SimpleKey.EMPTY);
    }
    
    @Test
    public void testNonCollectionSingleParam() {
        Object obj1 = new Object();
        Object key = CollectionSafeKeyGenerator.generateKey(obj1);
        assertSame(key, obj1);
    }
    
    @Test
    public void testNonCollectionMultiParam() {
        Object obj1 = new Object();
        Object obj2 = new Object();
        Object key = CollectionSafeKeyGenerator.generateKey(obj1, obj2);
        assertTrue(key instanceof SimpleKey);
        assertEquals(new SimpleKey(obj1, obj2), key);
    }
    
    @Test
    public void testCollectionSingleParam() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        Object key = CollectionSafeKeyGenerator.generateKey(list);
        assertNotSame(key, list);
        assertEquals(key, list);
    }
    
    @Test
    public void testCollectionMultiParam() throws IllegalAccessException, NoSuchFieldException {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        HashSet<Object> set = new HashSet<>();
        set.add(new Object());
        Object key = CollectionSafeKeyGenerator.generateKey(list, set);
        assertTrue(key instanceof SimpleKey);
        assertEquals(new SimpleKey(list, set), key);
        // extract the SimpleKey params and test those
        Field field = SimpleKey.class.getDeclaredField("params");
        field.setAccessible(true);
        Object[] params = (Object[]) field.get(key);
        assertEquals(2, params.length);
        assertNotSame(params[0], list);
        assertEquals(params[0], list);
        assertNotSame(params[1], set);
        assertEquals(params[1], set);
    }
    
    @Test
    public void testMixedMultiParam() throws IllegalAccessException, NoSuchFieldException {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object());
        Object obj1 = new Object();
        HashSet<Object> set = new HashSet<>();
        set.add(new Object());
        Object obj2 = new Object();
        Object key = CollectionSafeKeyGenerator.generateKey(list, obj1, set, obj2);
        assertTrue(key instanceof SimpleKey);
        assertEquals(new SimpleKey(list, obj1, set, obj2), key);
        // extract the SimpleKey params and test those
        Field field = SimpleKey.class.getDeclaredField("params");
        field.setAccessible(true);
        Object[] params = (Object[]) field.get(key);
        assertEquals(4, params.length);
        assertNotSame(params[0], list);
        assertEquals(params[0], list);
        assertSame(params[1], obj1);
        assertNotSame(params[2], set);
        assertEquals(params[2], set);
        assertSame(params[3], obj2);
    }
    
}
