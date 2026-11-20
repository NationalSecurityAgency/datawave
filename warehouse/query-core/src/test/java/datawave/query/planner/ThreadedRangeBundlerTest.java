package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Test;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.CloseableIterable;
import datawave.query.jexl.JexlASTHelper;

public class ThreadedRangeBundlerTest {

    @Test
    public void whenInstantiatingViaDefaultBuilder_thenDefaultValuesAreSet() {
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder().build();

        assertNull(bundler.getOriginal());
        assertNull(bundler.getRanges());
        assertEquals(0L, bundler.getMaxRanges());
        assertNull(bundler.getSettings());
        assertNull(bundler.getQueryTree());
        assertNull(bundler.getQueryPlanComparators());
    }

    @Test
    public void whenInstantiatingViaModifiedBuilder_thenSpecifiedValuesAreSet() throws Exception {
        QueryData original = new QueryData();
        CloseableIterable<QueryPlan> ranges = mock(CloseableIterable.class);
        Query settings = new QueryImpl();
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery("FIELD == 'value'");
        Collection<Comparator<QueryPlan>> queryPlanComparators = Collections.singletonList(Comparator.comparing(QueryPlan::getQueryString));

        // @formatter:off
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder()
                        .setOriginal(original)
                        .setRanges(ranges)
                        .setMaxRanges(100)
                        .setSettings(settings)
                        .setQueryTree(queryTree)
                        .setQueryPlanComparators(queryPlanComparators)
                        .build();
        // @formatter:on

        assertEquals(original, bundler.getOriginal());
        assertEquals(ranges, bundler.getRanges());
        assertEquals(100L, bundler.getMaxRanges());
        assertEquals(settings, bundler.getSettings());
        assertEquals(queryTree, bundler.getQueryTree());
        assertEquals(queryPlanComparators, bundler.getQueryPlanComparators());
    }

    @Test
    public void whenIteratorIsCalledMoreThanOnce_thenExceptionIsThrown() throws NoSuchFieldException, IllegalAccessException {
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder().build();

        // Set the iterator field to a non-null mock to mimic iterator() being previously called.
        ThreadedRangeBundlerIterator iterator = mock(ThreadedRangeBundlerIterator.class);
        setIterator(bundler, iterator);

        IllegalStateException exception = assertThrows(IllegalStateException.class, bundler::iterator);
        assertTrue(exception.getMessage().contains("iterator() was already called once"));
    }

    @Test
    public void whenCloseIsCalled_thenUnderlyingIteratorIsCalled() throws NoSuchFieldException, IllegalAccessException, IOException {
        ThreadedRangeBundler bundler = ThreadedRangeBundler.builder().build();

        // Expect a call to iterator.close().
        ThreadedRangeBundlerIterator iterator = mock(ThreadedRangeBundlerIterator.class);

        setIterator(bundler, iterator);

        bundler.close();
        verify(iterator, times(1)).close();

        verifyNoMoreInteractions(iterator);
    }

    private void setIterator(final ThreadedRangeBundler bundler, final ThreadedRangeBundlerIterator iterator)
                    throws NoSuchFieldException, IllegalAccessException {
        Field field = bundler.getClass().getDeclaredField("iterator");
        field.setAccessible(true);
        field.set(bundler, iterator);
    }
}
