package datawave.query.util.sortedset;

import org.apache.accumulo.core.data.Key;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferedFileBackedKeySortedSetTest {
    
    private final List<SortedSetTempFileHandler> tempFileHandlers = new ArrayList<>();
    private Key[] data = null;
    private int[] sortedOrder = null;
    private BufferedFileBackedSortedSet<Key> set = null;
    
    @Before
    public void setUp() throws Exception {
        byte[] template = new byte[] {5, 2, 78, 4, 8, 3, 54, 23, 6, 21, 7, 16};
        int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};
        data = new Key[template.length * 2];
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            Arrays.fill(buffer, template[i]);
            data[i] = new Key(buffer);
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[10];
            Arrays.fill(buffer, template[i]);
            data[i + template.length] = new Key(buffer);
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        set = new BufferedFileBackedSortedSet<>(null, 5, 7, 2, Collections.singletonList(new BufferedFileBackedSortedSet.SortedSetFileHandlerFactory() {
            @Override
            public FileSortedSet.SortedSetFileHandler createHandler() throws IOException {
                SortedSetTempFileHandler fileHandler = new SortedSetTempFileHandler();
                tempFileHandlers.add(fileHandler);
                return fileHandler;
            }
            
            @Override
            public boolean isValid() {
                return true;
            }
        }), new FileKeySortedSet.Factory());
        
        // adding in the data set multiple times to create underlying files with duplicate values making the
        // MergeSortIterator's job a little tougher...
        for (int d = 0; d < 11; d++) {
            Collections.addAll(set, data);
        }
    }
    
    @After
    public void tearDown() throws Exception {
        // Delete each sorted set file and its checksum.
        for (SortedSetTempFileHandler fileHandler : tempFileHandlers) {
            File file = fileHandler.getFile();
            tryDelete(file);
            File checksum = new File(file.getParent(), "." + file.getName() + ".crc");
            tryDelete(checksum);
        }
        tempFileHandlers.clear();
        
        data = null;
        sortedOrder = null;
        set.clear();
        set = null;
    }
    
    private void tryDelete(File file) {
        if (file.exists()) {
            Assert.assertTrue("Failed to delete file " + file, file.delete());
        }
    }
    
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
    
    @Test
    public void testClear() {
        set.clear();
        assertTrue(set.isEmpty());
    }
    
    @Test
    public void testContainsObject() {
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
        }
        for (int i = 1; i < (data.length / 2); i++) {
            assertTrue(set.contains(data[i]));
        }
        for (int i = (data.length / 2); i < data.length; i++) {
            assertFalse(set.contains(data[i]));
        }
    }
    
    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<Key> it = set.iterator(); it.hasNext();) {
            Key value = it.next();
            Key expected = data[sortedOrder[index++]];
            assertEquals(expected, value);
        }
        set.clear();
        for (Key value : set) {
            fail();
        }
    }
    
    @Test
    public void testIteratorRemove() {
        int size = set.size();
        int failCount = 0;
        assertFalse(set.isPersisted());
        // calling iterator() will force persistence
        for (Iterator<Key> it = set.iterator(); it.hasNext();) {
            assertTrue(set.isPersisted());
            Key value = it.next();
            assertTrue(set.contains(value));
            try {
                it.remove();
                fail("Expected iterator remove to fail with a persisted set");
            } catch (Exception e) {
                // expected that some of the underlying FileSortedSets are persisted and hence the remove will fail
                failCount++;
                assertTrue(set.contains(value));
                assertEquals(size, set.size());
            }
        }
        assertEquals(size, failCount);
        assertFalse(set.isEmpty());
    }
    
    @Test
    public void testSubSet() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        try {
            SortedSet<Key> subSet = set.subSet(data[sortedOrder[start]], data[sortedOrder[end]]);
            SortedSet<Key> expected = new TreeSet<>();
            for (int i = start; i < end; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, subSet);
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testHeadSet() {
        int end = sortedOrder.length / 3;
        try {
            SortedSet<Key> subSet = set.headSet(data[sortedOrder[end]]);
            SortedSet<Key> expected = new TreeSet<>();
            for (int i = 0; i < end; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, subSet);
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testTailSet() {
        int start = sortedOrder.length / 3;
        try {
            SortedSet<Key> subSet = set.tailSet(data[sortedOrder[start]]);
            SortedSet<Key> expected = new TreeSet<>();
            for (int i = start; i < sortedOrder.length; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, subSet);
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testLast() {
        Key expected = data[sortedOrder[data.length - 1]];
        Key value = set.last();
        assertEquals(expected, value);
    }
    
    @Test
    public void testFirst() {
        Key expected = data[sortedOrder[0]];
        Key value = set.first();
        assertEquals(expected, value);
    }
    
    @Test
    public void testCompaction() throws IOException {
        assertEquals(8, set.getSets().size());
        set.persist();
        assertEquals(3, set.getSets().size());
    }
    
}
