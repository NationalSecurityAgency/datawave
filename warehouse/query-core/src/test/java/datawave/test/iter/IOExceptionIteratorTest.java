package datawave.test.iter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.util.SortedMapIterator;
import datawave.core.iterators.IteratorTimeoutException;

public class IOExceptionIteratorTest {

    private final Value EMPTY_VALUE = new Value();

    @Test
    public void testNoExceptionClassOption() {
        Map<String,String> options = Collections.emptyMap();
        initializeIteratorWithOptions(options);
    }

    @Test
    public void testNoExceptionMessageOption() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        initializeIteratorWithOptions(options);
    }

    @Test
    public void testNoFireOption() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "NPE for test");
        initializeIteratorWithOptions(options);
    }

    private void initializeIteratorWithOptions(Map<String,String> options) {
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(Collections.emptySortedMap());
        IOExceptionIterator iter = new IOExceptionIterator();
        assertThrows(IllegalStateException.class, () -> iter.init(source, options, new TestIteratorEnv()));
    }

    @Test
    public void testExceptionOnSeek() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(IOExceptionIterator.FIRE_ON_SEEK, "true");
        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnNext() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(IOExceptionIterator.FIRE_ON_NEXT, "true");
        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnRandomOperation() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "IOException msg");
        options.put(IOExceptionIterator.FIRE_RANDOMLY, "true");
        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnModulo_NoModulusSpecified() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "IOException msg");
        options.put(IOExceptionIterator.FIRE_MODULO, "true");

        Exception e = assertThrows(IllegalArgumentException.class, () -> driveIterator(options, 5));
        String expected = "FIRE_MODULO was set without a MODULUS option";
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void testExceptionOnModulo_ExceptionDoesNotFire() throws Exception {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "IOException msg");
        options.put(IOExceptionIterator.FIRE_MODULO, "true");
        options.put(IOExceptionIterator.MODULUS, "100");

        driveIterator(options, 5);
    }

    @Test
    public void testExceptionOnModulo() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "IOException msg");
        options.put(IOExceptionIterator.FIRE_MODULO, "true");
        options.put(IOExceptionIterator.MODULUS, "5");

        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    /**
     * Drive the iterator across a dataset with default size of 50
     *
     * @param options
     *            the map of iterator options
     * @throws Exception
     *             if something goes wrong
     */
    private void driveIterator(Map<String,String> options) throws Exception {
        driveIterator(options, 50);
    }

    /**
     * Drive the iterator across a dataset of specified size
     *
     * @param options
     *            the map of iterator options
     * @param size
     *            the number of keys in the dataset
     * @throws Exception
     *             if something goes wrong
     */
    private void driveIterator(Map<String,String> options, int size) throws Exception {
        SortedKeyValueIterator<Key,Value> source = getSource(size);
        IOExceptionIterator iter = new IOExceptionIterator();
        iter.init(source, options, new TestIteratorEnv());
        iter.seek(new Range(), Collections.emptySet(), false);
        while (iter.hasTop()) {
            iter.next();
        }
    }

    /**
     * Create a {@link SortedMapIterator} of the specified size
     *
     * @param size
     *            the number of keys
     * @return an iterator
     */
    private SortedKeyValueIterator<Key,Value> getSource(int size) {
        return new SortedMapIterator(getData(size));
    }

    /**
     * Create a dataset of specified size
     *
     * @param count
     *            the size of the dataset
     * @return a dataset
     */
    private TreeMap<Key,Value> getData(int count) {
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            data.put(new Key("row-" + i), EMPTY_VALUE);
        }
        return data;
    }
}
