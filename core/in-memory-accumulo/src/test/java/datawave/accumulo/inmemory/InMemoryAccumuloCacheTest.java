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
        InMemoryAccumulo first = InMemoryAccumuloCache.getOrCreate("shared");
        first.createTable("root", "sharedTable", true, TimeType.LOGICAL);

        InMemoryAccumulo second = InMemoryAccumuloCache.getOrCreate("shared");
        assertSame(first, second);
        assertTrue(new InMemoryAccumuloClient("root", second).tableOperations().exists("sharedTable"));
    }

    @Test
    public void differentNamesReturnDifferentInstances() {
        assertNotSame(InMemoryAccumuloCache.getOrCreate("one"), InMemoryAccumuloCache.getOrCreate("two"));
        assertEquals(2, InMemoryAccumuloCache.size());
    }

    @Test
    public void removedInstanceIsReplacedWithANewOne() {
        InMemoryAccumulo first = InMemoryAccumuloCache.getOrCreate("removed");
        assertSame(first, InMemoryAccumuloCache.remove("removed"));
        assertNull(InMemoryAccumuloCache.remove("removed"));
        assertNotSame(first, InMemoryAccumuloCache.getOrCreate("removed"));
    }

    @Test
    public void clearRemovesEveryInstance() {
        InMemoryAccumuloCache.getOrCreate("one");
        InMemoryAccumuloCache.getOrCreate("two");
        InMemoryAccumuloCache.clear();
        assertEquals(0, InMemoryAccumuloCache.size());
    }
}
