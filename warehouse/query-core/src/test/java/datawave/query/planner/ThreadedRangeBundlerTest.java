package datawave.query.planner;

import datawave.query.CloseableIterable;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.QueryData;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Comparator;

import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThreadedRangeBundlerTest {
    
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
    @Test
    public void whenInstantiatingViaDefaultBuilder_thenDefaultValuesAreSet() {
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder().build();
        
        assertNull(bundler.getOriginal());
        assertNull(bundler.getRanges());
        assertEquals(0L, bundler.getMaxRanges());
        assertNull(bundler.getSettings());
        assertNull(bundler.getQueryTree());
        assertFalse(bundler.isDocSpecificLimitOverride());
        assertEquals(-1, bundler.getDocsToCombine());
        assertNull(bundler.getQueryPlanComparators());
        assertEquals(0, bundler.getNumRangesToBuffer());
        assertEquals(0L, bundler.getRangeBufferTimeoutMillis());
        assertEquals(100L, bundler.getRangeBufferPollMillis());
        assertEquals(50L, bundler.getMaxRangeWaitMillis());
    }
    
    @Test
    public void whenInstantiatingViaModifiedBuilder_thenSpecifiedValuesAreSet() {
        QueryData original = mock(QueryData.class);
        CloseableIterable<QueryPlan> ranges = mock(CloseableIterable.class);
        Query settings = mock(Query.class);
        ASTJexlScript queryTree = mock(ASTJexlScript.class);
        Collection<Comparator<QueryPlan>> queryPlanComparators = mock(Collection.class);
        
        // @formatter:off
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder()
                        .setOriginal(original)
                        .setRanges(ranges)
                        .setMaxRanges(100)
                        .setSettings(settings)
                        .setQueryTree(queryTree)
                        .setDocSpecificLimitOverride(true)
                        .setDocsToCombine(10)
                        .setMaxRangeWaitMillis(1)
                        .setQueryPlanComparators(queryPlanComparators)
                        .setNumRangesToBuffer(1)
                        .setRangeBufferTimeoutMillis(10)
                        .setRangeBufferPollMillis(5)
                        .build();
        // @formatter:on
        
        assertEquals(original, bundler.getOriginal());
        assertEquals(ranges, bundler.getRanges());
        assertEquals(100L, bundler.getMaxRanges());
        assertEquals(settings, bundler.getSettings());
        assertEquals(queryTree, bundler.getQueryTree());
        assertTrue(bundler.isDocSpecificLimitOverride());
        assertEquals(10, bundler.getDocsToCombine());
        assertEquals(queryPlanComparators, bundler.getQueryPlanComparators());
        assertEquals(1, bundler.getNumRangesToBuffer());
        assertEquals(10L, bundler.getRangeBufferTimeoutMillis());
        assertEquals(5L, bundler.getRangeBufferPollMillis());
        assertEquals(1L, bundler.getMaxRangeWaitMillis());
    }
    
    @Test
    public void whenIteratorIsCalledMoreThanOnce_thenExceptionIsThrown() throws NoSuchFieldException, IllegalAccessException {
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder().build();
        
        // Set the iterator field to a non-null mock to mimic iterator() being previously called.
        ThreadedRangeBundlerIterator iterator = mock(ThreadedRangeBundlerIterator.class);
        setIterator(bundler, iterator);
        
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("iterator() was already called once");
        bundler.iterator();
    }
    
    @Test
    public void whenCloseIsCalled_thenUnderlyingIteratorIsCalled() throws NoSuchFieldException, IllegalAccessException, IOException {
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder().build();
        
        // Expect a call to iterator.close().
        ThreadedRangeBundlerIterator iterator = mock(ThreadedRangeBundlerIterator.class);
        iterator.close();
        replay(iterator);
        setIterator(bundler, iterator);
        
        bundler.close();
        verify(iterator);
    }
    
    private void setIterator(final ThreadedRangeBundler bundler, final ThreadedRangeBundlerIterator iterator) throws NoSuchFieldException,
                    IllegalAccessException {
        Field field = bundler.getClass().getDeclaredField("iterator");
        field.setAccessible(true);
        field.set(bundler, iterator);
    }
}
