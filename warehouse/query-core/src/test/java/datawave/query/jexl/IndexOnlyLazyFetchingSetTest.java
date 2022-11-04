package datawave.query.jexl;

import datawave.query.iterator.IndexOnlyFunctionIterator;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(EasyMockExtension.class)
public class IndexOnlyLazyFetchingSetTest extends EasyMockSupport {
    
    @Mock
    IndexOnlyFunctionIterator<Object> parentIterator;
    
    @Mock
    Iterator<Object> fetchingIterator;
    
    @Test
    public void testAddAll_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        String result1 = subject.toString();
        boolean result2 = subject.addAll(Collections.singletonList("value4"));
        String result3 = subject.toString();
        verifyAll();
        
        // Verify results
        assertTrue(result1.contains("unfetched"), "toString should have indicated that the values had not been fetched");
        
        assertTrue(result2, "Addition should have been successful");
        
        assertTrue(result3.contains("unfetched"), "toString should have indicated that the values still had not been fetched");
    }
    
    @Test
    public void testClear_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        
        // Set expectations, looping twice to simulate the 2 fetch sequences occurring before and after the
        // clear() invocation. Although size() is called 3 times, only the first and last should result in a
        // fetch sequence.
        for (int i = 0; i < 2; i++) {
            Iterator<Object> iterator = values.iterator();
            expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
            while (iterator.hasNext()) {
                expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
                expect(this.fetchingIterator.next()).andReturn(iterator.next());
                expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            }
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        String result1 = subject.getFieldName();
        int result2 = subject.size();
        int result3 = subject.size();
        subject.clear();
        int result4 = subject.size();
        verifyAll();
        
        // Verify results
        assertEquals(fieldName, result1, "The field name should have been returned from the accessor");
        
        assertEquals(values.size(), result2, "Incorrect number of values fetched before the clear");
        
        assertEquals(values.size(), result3, "Incorrect number of values fetched before the clear, which should NOT have resulted in a fetch operation");
        
        assertEquals(values.size(), result4, "Incorrect number of values fetched after the clear");
    }
    
    @Test
    public void testContains_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        boolean result1 = subject.contains("value3");
        boolean result2 = subject.contains("value1");
        verifyAll();
        
        // Verify results
        assertTrue(result1, "The subject should have contained the fetched value");
        
        assertTrue(result2, "The subject should have contained the fetched value without forcing any more fetching");
    }
    
    @Test
    public void testContainsAll_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        boolean result1 = subject.containsAll(Arrays.asList("value3", "value1"));
        verifyAll();
        
        // Verify results
        assertTrue(result1, "The subject should have contained the fetched values");
    }
    
    @Test
    public void testIterator_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        
        // Set expectations
        for (int i = 0; i < 2; i++) {
            Iterator<Object> iterator = values.iterator();
            expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
            while (iterator.hasNext()) {
                expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
                expect(this.fetchingIterator.next()).andReturn(iterator.next());
                
                // hasNext() called a second time when fetched values are retained in memory
                if (i > 0) {
                    expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
                }
            }
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        
        // Run the test
        replayAll();
        
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        Iterator<String> result1 = subject.iterator(); // Trigger a simulated fetch. By default, iterated values are not
        int result2 = 0; // retained in memory.
        for (; result1.hasNext(); result2++) {
            result1.next();
        }
        String result3 = subject.toString();
        
        subject.setKeepIteratedValuesInMemory(true); // Trigger a simulated fetch. However, allow iterated values to be
        Iterator<String> result4 = subject.iterator(); // retained in memory.
        int result5 = 0;
        for (; result4.hasNext(); result5++) {
            result4.next();
        }
        String result6 = subject.toString();
        
        Iterator<String> result7 = subject.iterator();
        int result8 = 0;
        for (; result7.hasNext(); result8++) {
            result7.next();
        }
        
        verifyAll();
        
        // Verify results
        assertNotNull(result1, "The subject should have returned a non-null iterator");
        
        assertEquals(values.size(), result2, "Incorrect number of values fetched via the iterator");
        
        assertTrue(result3.contains("unfetched"), "The subject, by default, should NOT have retained fetched values");
        
        assertNotNull(result4, "The subject should have returned a non-null iterator");
        
        assertEquals(values.size(), result5, "Incorrect number of values fetched via the iterator");
        
        assertFalse(result6.contains("unfetched"), "The subject, by setting the flag, should have retained fetched values");
        
        assertNotNull(result7, "The subject should have returned a non-null iterator, but all fetching should have occurred using only the previous instance");
        
        assertEquals(values.size(), result8, "Incorrect number of in-memory values obtained from the iterator");
    }
    
    @Test
    public void testRemove_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        boolean result1 = subject.remove("value3");
        verifyAll();
        
        // Verify results
        assertTrue(result1, "The subject should have removed the fetched value");
    }
    
    @Test
    public void testRemoveAll_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        boolean result1 = subject.removeAll(Arrays.asList("value3", "value1"));
        verifyAll();
        
        // Verify results
        assertTrue(result1, "The subject should have removed the fetched values");
    }
    
    @Test
    public void testRetainAll_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        boolean result1 = subject.retainAll(Arrays.asList("value3", "value1"));
        verifyAll();
        
        // Verify results
        assertTrue(result1, "The subject should have retained the fetched values");
    }
    
    @Test
    public void testIsEmpty_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>();
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        subject.setKeepIteratedValuesInMemory(true);
        boolean result1 = subject.isEmpty();
        boolean result2 = subject.add("test");
        verifyAll();
        
        // Verify results
        assertTrue(result1, "The subject should indicate that it is empty (the fetch occurred but returned an empty set)");
        
        assertTrue(result2, "The subject should indicate that an in-memory add occurred");
        
    }
    
    @Test
    public void testToArray_HappyPath() {
        // Create test input
        String fieldName = "HEAD";
        Collection<Object> values = new HashSet<>(Arrays.asList("value1", new StringBuilder("value2"), "value3"));
        Iterator<Object> iterator = values.iterator();
        
        // Set expectations
        expect(this.parentIterator.newLazyFetchingIterator(fieldName)).andReturn(this.fetchingIterator);
        while (iterator.hasNext()) {
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.fetchingIterator.next()).andReturn(iterator.next());
            expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        }
        expect(this.fetchingIterator.hasNext()).andReturn(iterator.hasNext());
        
        // Run the test
        replayAll();
        IndexOnlyLazyFetchingSet<String,Object> subject = new IndexOnlyLazyFetchingSet<>(fieldName, this.parentIterator);
        Object[] result1 = subject.toArray();
        Object[] result2 = subject.toArray(new Object[0]);
        verifyAll();
        
        // Verify results
        assertNotNull(result1, "The subject should have returned a non-null array of fetched values");
        
        assertNotNull(result2, "The subject should have returned a non-null array of fetched values");
    }
}
