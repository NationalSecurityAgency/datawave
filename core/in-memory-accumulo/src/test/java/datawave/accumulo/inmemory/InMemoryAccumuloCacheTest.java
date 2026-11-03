package datawave.accumulo.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.client.admin.TimeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class InMemoryAccumuloCacheTest {

    @AfterEach
    public void clearCache() {
        InMemoryAccumuloCache.clear();
    }

    @Test
    public void sameNameReturnsTheSameInstance() {
        InMemoryAccumulo first = InMemoryAccumuloCache.getCachedInstance("shared");
        first.createTable("root", "sharedTable", true, TimeType.LOGICAL);

        InMemoryAccumulo second = InMemoryAccumuloCache.getCachedInstance("shared");
        assertSame(first, second);
        assertTrue(new InMemoryAccumuloClient("root", second).tableOperations().exists("sharedTable"));
    }

    @Test
    public void differentNamesReturnDifferentInstances() {
        assertNotSame(InMemoryAccumuloCache.getCachedInstance("one"), InMemoryAccumuloCache.getCachedInstance("two"));
        assertEquals(2, InMemoryAccumuloCache.size());
    }

    @Test
    public void removedInstanceIsReplacedWithANewOne() {
        InMemoryAccumulo first = InMemoryAccumuloCache.getCachedInstance("removed");
        assertSame(first, InMemoryAccumuloCache.removeCachedInstance("removed"));
        assertNull(InMemoryAccumuloCache.removeCachedInstance("removed"));
        assertNotSame(first, InMemoryAccumuloCache.getCachedInstance("removed"));
    }

    @Test
    public void clearRemovesEveryInstance() {
        InMemoryAccumuloCache.getCachedInstance("one");
        InMemoryAccumuloCache.getCachedInstance("two");
        InMemoryAccumuloCache.clear();
        assertEquals(0, InMemoryAccumuloCache.size());
    }
}
